package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * Stores the location of each replica for a give table, including the update ID for each of these replicas
 * (stating the last time a replica was updated), and the set of replicas that are currently active (i.e. up-to-date).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaManager {

	/**
	 * Set of databases which hold replicas for this table. All replica locations are
	 * held here.
	 * 
	 * <p>Key: Location of the replica
	 * <p>Value: Number given to the last update made at that replica.
	 */
	private Map<DatabaseInstanceRemote, Integer> allReplicas;

	/**
	 * The set of replicas that are currently active - i.e. up-to-date. Queries can only
	 * be executed on this set of replicas.
	 */
	private Set<DatabaseInstanceRemote> activeReplicas;

	/**
	 * The database instance which is running this data manager. 
	 */
	private DatabaseInstanceRemote primaryLocation;

	/**
	 * Number given to the last update to a replica.
	 */
	private int lastUpdate;


	public ReplicaManager(){
		this.allReplicas = new HashMap<DatabaseInstanceRemote, Integer>();
		this.activeReplicas = new HashSet<DatabaseInstanceRemote>();
		this.primaryLocation = null;

		this.lastUpdate = 0;
	}

	public void add(DatabaseInstanceRemote replicaLocation){
		if (primaryLocation == null){
			primaryLocation = replicaLocation;
		}

		allReplicas.put(replicaLocation, lastUpdate);
		activeReplicas.add(replicaLocation);
	}

	/**
	 * @param instance
	 * @param updateID
	 */
	public void add(DatabaseInstanceRemote instance, int updateID) {
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
	public DatabaseInstanceRemote getPrimary() {
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
	public Set<DatabaseInstanceRemote> getActiveReplicas() {
		return activeReplicas;
	}

	/**
	 * Remove a database instance as a location of a stored replica.
	 * @param createFullDatabaseLocation
	 */
	public void remove(DatabaseInstanceRemote dbInstance) {
		allReplicas.remove(dbInstance);

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
	public void completeUpdate(Set<DatabaseInstanceRemote> updatedReplicas, int updateID) {

		if (updatedReplicas != null && updatedReplicas.size() != 0){

			activeReplicas = updatedReplicas;

			/*
			 * Loop through each replica which was updated, re-adding them into the
			 * replicaLocations hashmap along with the new updateID.
			 */
			for (DatabaseInstanceRemote instance: updatedReplicas){
				if (allReplicas.containsKey(instance)){
					Integer previousID = allReplicas.get(instance);

					if (updateID <= previousID){
						ErrorHandling.hardError("Unexpected code path 1.");
					}

					allReplicas.remove(instance);
					allReplicas.put(instance, updateID);

				} else {
					/*
					 * This replica was never on this machine.
					 * 
					 */
					try {
						ErrorHandling.errorNoEvent("Instance does not hold replica: " + instance.getConnectionString());
					} catch (RemoteException e) {
						e.printStackTrace();
					}
					//assert false; //This else should never be entered.
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
}
