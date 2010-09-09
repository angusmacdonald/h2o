﻿/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.replication;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Stores the location of each replica for a give table, including the update ID for each of these replicas (stating the last time a replica
 * was updated), and the set of replicas that are currently active (i.e. up-to-date).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaManager implements Serializable {

	private static final long serialVersionUID = 6064010173578943054L;

	/**
	 * Set of databases which hold replicas for this table. All replica locations are held here.
	 * 
	 * <p>
	 * Key: Location of the replica
	 * <p>
	 * Value: Number given to the last update made at that replica.
	 */
	private Map<DatabaseInstanceWrapper, Integer> allReplicas;

	/**
	 * The set of replicas that are currently active - i.e. up-to-date. Queries can only be executed on this set of replicas.
	 */
	private Map<DatabaseInstanceWrapper, Integer> activeReplicas;

	/**
	 * The database instance which is running this Table Manager.
	 */
	private DatabaseInstanceWrapper primaryLocation;

	public ReplicaManager() {
		this.allReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
		this.activeReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
		this.primaryLocation = null;

	}

	/**
	 * Add a new replica to the active set of replicas. It will be given the latest update ID recorded.
	 * 
	 * @param replicaLocation
	 */
	public void add(DatabaseInstanceWrapper replicaLocation) {

		assert replicaLocation != null;

		if (primaryLocation == null) {
			primaryLocation = replicaLocation;
		}

		addToAllReplicas(replicaLocation, getCurrentUpdateID());
		activeReplicas.put(replicaLocation, getCurrentUpdateID());
	}

	private Integer addToAllReplicas(DatabaseInstanceWrapper replicaLocation, Integer newUpdateID) {

		return getAllReplicas().put(replicaLocation, newUpdateID);
	}

	public int getCurrentUpdateID() {
		for (Integer updateID: activeReplicas.values()){

			return updateID; // all the update IDs will be the same because all these replicas are active.
		}

		return 0; // will return this for inserts where there are not yet any active replicas.
	}

	/**
	 * Add a set of databases to the active set of replicas. This just calls the {@link #add(DatabaseInstanceWrapper)} method on each
	 * replica location in the list.
	 * 
	 * @param replicaLocations
	 *            a number of replica locations.
	 */
	public void add(List<DatabaseInstanceWrapper> replicaLocations) {

		assert replicaLocations != null;

		if (replicaLocations.size() == 0) {
			return;
		}

		for (DatabaseInstanceWrapper diw : replicaLocations) {
			add(diw);
		}
	}

	/**
	 * @return The number of replicas for this table. Some may not currently be up-to-date.
	 */
	public int allReplicasSize() {
		return getAllReplicas().size();
	}


	/**
	 * @return
	 */
	public Map<DatabaseInstanceWrapper, Integer> getActiveReplicas() {
		return activeReplicas;
	}

	/**
	 * Remove a database instance as a location of a stored replica.
	 * 
	 * @param createFullDatabaseLocation
	 */
	public void remove(DatabaseInstanceRemote dbInstance) {
		getAllReplicas().remove(dbInstance);
		activeReplicas.remove(dbInstance);
	}


	/**
	 * Finish an update by revising the set of replica locations with new information on:
	 * <p>
	 * The updateID of the last update committed on each replica.
	 * <p>
	 * The set of replicas which are deemed active.
	 * @param commit True if all replicas are committing.
	 * 
	 * @param committedQueries	The queries which are to be committed.
	 * @param synchronousUpdate
	 * @return	The set of queries that were actually committed. Those that were on incoming list of 
	 * committed queries will be returned if their update IDs match up. If they don't appear on the returned list
	 * then they are no longer active replicas.
	 */
	public List<CommitResult> completeUpdate(boolean commit, Set<CommitResult> committedQueries, boolean synchronousUpdate, TableInfo tableInfo) {

		List<CommitResult> successfullyCommittedQueries = new LinkedList<CommitResult>(); // queries that were successfully committed here.

		int updateID = getUpdateIDFromCommittedQueries(committedQueries, tableInfo);

		/*
		 * Check whether all updates were rollbacks. If they were there is no need to remove any replicas
		 * from the set of active replicas.
		 */
		boolean allRollback = false;

		if (!commit){
			if (committedQueries == null){
				allRollback = true;
			} else {
				for (CommitResult commitResult: committedQueries){
					if (!commitResult.isCommit()) {
						allRollback = true;
						break;
					}
				}
			}
		}

		if (committedQueries != null && committedQueries.size() != 0) {

			
			if (!allRollback){
				//Reset the active set.
				activeReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
			}
			
			Set<DatabaseInstanceWrapper> instancesUpdated = new HashSet<DatabaseInstanceWrapper>();
			
			/*
			 * Loop through each replica which was updated, re-adding them into the replicaLocations hashmap along with the new
			 * updateID.
			 */
			
			for (CommitResult commitResult : committedQueries) {
				
				DatabaseInstanceWrapper wrapper = commitResult.getDatabaseInstanceWrapper();
				
				if (instancesUpdated.contains(wrapper)) continue; //don't update info for the same replica twice.
				
				/*
				 * Meaning of IF statement:
				 * IF this replica location is actually a replica location for this table
				 * AND
				 * the commit information is for this table OR no table is specified (which is the case for queries which have bypassed the asynchronous update
				 * manager.
				 * THEN...
				 * Update the active/all replica set with new update IDs where appropriate.
				 */
				if (getAllReplicas().containsKey(wrapper) && (tableInfo.equals(commitResult.getTable()) || commitResult.getTable() == null)) {

					instancesUpdated.add(wrapper);
					
					final Integer previousID = getAllReplicas().get(wrapper);

					if (updateID == previousID) {
						/*
						 * The updateID of this current replica equals the update ID that was expected of this replica at this point.
						 * Commit can proceed.
						 */

						int newUpdateID = previousID + 1;

						if (commitResult.isCommit()) {
							activeReplicas.put(wrapper, newUpdateID);
							addToAllReplicas(wrapper, newUpdateID);

							successfullyCommittedQueries.add(commitResult); //this query has been successfully updated.
						} else {
							if (!allRollback){
								/*
								 * Only remove replicas in case of rollback if some of the replicas managed to commit.
								 * Otherwise they are all still in a consistent state and all still active.
								 */
								activeReplicas.remove(wrapper);
								getAllReplicas().remove(wrapper);
							}
						}

					} else {
						/*
						 * The update ID of this replica does not match that which was expected. This replica will not commit.
						 */
						ErrorHandling.errorNoEvent("Replica will not commit because update IDs did not match. Expected: " + updateID + "; Actual current: " + previousID);
					}

				} // In many cases it won't contain this key, but another table (part of the same transaction) was on this machine.
				else {
					//ErrorHandling.errorNoEvent("Update wasn't applicable to this table " + tableInfo); - valid state to be in...
				}
			}
		}
		
		return successfullyCommittedQueries;
	}

	private int getUpdateIDFromCommittedQueries(Collection<CommitResult> committedQueries, TableInfo tableInfo) {
		int updateID = 0;
		
		if (committedQueries == null){
			return 0 ;
		}
		
		for (CommitResult cr: committedQueries){
			//XXX should expected update ID always be the same?
			try {
				if (cr.getExpectedUpdateID() > updateID && 
						(((cr.getTable() != null && tableInfo != null) && cr.getTable().equals(tableInfo)) ) ){
					updateID = cr.getExpectedUpdateID();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return updateID;
	}

	/**
	 * Whether every replica contains the most recent copy of the data.
	 * 
	 * @return True if every replica is deemed active - false, if some are inactive because they don't contain the latest updates.
	 */
	public boolean areReplicasConsistent() {
		return (activeReplicas.size() == getAllReplicas().size());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ReplicaManager [number of replicas=" + getAllReplicas().size() + "]";
	}

	/**
	 * 
	 */
	public int getNumberOfReplicas() {
		return activeReplicas.size();
	}

	/**
	 * @return
	 */
	public String[] getReplicaLocationsAsStrings() {
		String[] locations = new String[activeReplicas.size()];

		int i = 0;

		locations[i++] = primaryLocation.getURL().getURLwithRMIPort(); // the primary location should always be first.

		for (DatabaseInstanceWrapper r : activeReplicas.keySet()) {
			if (r.equals(primaryLocation))
				continue;
			locations[i++] = r.getURL().getURLwithRMIPort();
		}

		return locations;
	}

	public void remove(Set<DatabaseInstanceWrapper> failed) {
		for (DatabaseInstanceWrapper wrapper : failed) {
			this.activeReplicas.remove(wrapper);
		}
	}

	public boolean contains(DatabaseInstanceWrapper databaseInstanceWrapper) throws RemoteException {
		return activeReplicas.containsKey(databaseInstanceWrapper);
	}

	public Map<DatabaseInstanceWrapper, Integer> getAllReplicas() {
		return allReplicas;
	}

}
