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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

/**
 * Stores the location of each replica for a give table, including the update ID for each of these replicas
 * (stating the last time a replica was updated), and the set of replicas that are currently active (i.e. up-to-date).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaManager implements Serializable {

	private static final long serialVersionUID = 6064010173578943054L;

	/**
	 * Set of databases which hold replicas for this table. All replica locations are
	 * held here.
	 * 
	 * <p>Key: Location of the replica
	 * <p>Value: Number given to the last update made at that replica.
	 */
	private Map<DatabaseInstanceWrapper, Integer> allReplicas;

	/**
	 * The set of replicas that are currently active - i.e. up-to-date. Queries can only
	 * be executed on this set of replicas.
	 */
	private Set<DatabaseInstanceWrapper> activeReplicas;

	/**
	 * The database instance which is running this Table Manager. 
	 */
	private DatabaseInstanceWrapper primaryLocation;

	/**
	 * Number given to the last update to a replica.
	 */
	private int lastUpdate;


	public ReplicaManager(){
		this.allReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();
		this.activeReplicas = new HashSet<DatabaseInstanceWrapper>();
		this.primaryLocation = null;

		this.lastUpdate = 0;
	}

	public void add(DatabaseInstanceWrapper replicaLocation){

		assert replicaLocation != null;

		if (primaryLocation == null){
			primaryLocation = replicaLocation;
		}

		allReplicas.put(replicaLocation, lastUpdate);
		activeReplicas.add(replicaLocation);
	}

	public void add(List<DatabaseInstanceWrapper> replicaLocations){

		assert replicaLocations != null;

		if (primaryLocation == null && replicaLocations.size() > 0){
			primaryLocation = replicaLocations.get(0);
		}

		for (DatabaseInstanceWrapper diw: replicaLocations){
			allReplicas.put(diw, lastUpdate);
		}

		activeReplicas.addAll(replicaLocations);
	}

	/**
	 * @param instance
	 * @param updateID
	 */
	public void add(DatabaseInstanceWrapper instance, int updateID) {
		allReplicas.put(instance, updateID);
	}

	/**
	 * @return
	 */
	public int size() {
		return allReplicas.size();
	}


	/**
	 * @return
	 */
	public int activeSize() {
		return activeReplicas.size();
	}
	
	/**
	 * @return
	 */
	public DatabaseInstanceWrapper getPrimary() {
		return primaryLocation;
	}

	/**
	 * @return
	 */
	public int getNewUpdateID() {
		this.lastUpdate ++;
		return this.lastUpdate;
	}

	/**
	 * @return
	 */
	public Set<DatabaseInstanceWrapper> getActiveReplicas() {
		return activeReplicas;
	}

	/**
	 * Remove a database instance as a location of a stored replica.
	 * @param createFullDatabaseLocation
	 */
	public void remove(DatabaseInstanceRemote dbInstance) {
		allReplicas.remove(dbInstance);
		activeReplicas.remove(dbInstance);
		//XXX the following code logic may not work when an instance is being removed then added with a new value.
		//		if (primaryLocation.equals(dbInstance)){
		//			if (replicaLocations.size()==0){
		//				ErrorHandling.hardError("All replicas have now been removed. DROP TABLE should have been called.");
		//			} else {
		//				ErrorHandling.hardError("New primary location needed - this isn't implemented yet."); //TODO implement.
		//			}
		//	
		//	}

	}

	/**
	 * Finish an update by revising the set of replica locations with new information on:
	 * 
	 * <p>The updateID of the last update committed on each replica.
	 * <p>The set of replicas which are deemed active.
	 */
	public void completeUpdate(Set<DatabaseInstanceWrapper> updatedReplicas, int updateID, boolean synchronousUpdate) {

		if (synchronousUpdate){
			//Don't change anything.

		} else {
			if (updatedReplicas != null && updatedReplicas.size() != 0){
				activeReplicas = updatedReplicas;

				/*
				 * Loop through each replica which was updated, re-adding them into the
				 * replicaLocations hashmap along with the new updateID.
				 */
				for (DatabaseInstanceWrapper instance: updatedReplicas){
					if (allReplicas.containsKey(instance)){
						Integer previousID = allReplicas.get(instance);

						assert updateID >= previousID;

						allReplicas.remove(instance);
						allReplicas.put(instance, updateID);

					} //In many cases it won't contain this key, but another table (part of the same transaction) was on this machine.
				}
			}
		}
	}

	/**
	 * Whether every replica contains the most recent copy of the data. 
	 * @return True if every replica is deemed active - false, if some are inactive
	 * because they don't contain the latest updates.
	 */
	public boolean areReplicasConsistent(){
		return (activeReplicas.size() == allReplicas.size());
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ReplicaManager [number of replicas=" + allReplicas.size() + "]";
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
	public String[] getReplicaLocations() {
		String[] locations = new String[activeReplicas.size()];

		int i = 0;
		
		locations[i++] = primaryLocation.getURL().getURLwithRMIPort(); //the primary location should always be first.
		
		for (DatabaseInstanceWrapper r: activeReplicas){
			if (r.equals(primaryLocation)) continue;
			locations[i++] = r.getURL().getURLwithRMIPort();
		}

		return locations;
	}

	public void remove(Set<DatabaseInstanceWrapper> failed) {
		this.activeReplicas.removeAll(failed);
	}


}
