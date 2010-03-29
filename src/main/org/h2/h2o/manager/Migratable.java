package org.h2.h2o.manager;

import java.rmi.RemoteException;

/**
 * Classes implementing this interface can be migrated to other machines in the system.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface Migratable {
	/**
	 * @throws MovedException 
	 * 
	 */
	public void prepareForMigration(String newLocation) throws RemoteException, MigrationException, MovedException;

	/**
	 * 
	 */
	public void checkConnection() throws RemoteException, MovedException;

	/**
	 * 
	 */
	public void completeMigration() throws RemoteException, MovedException, MigrationException ;

}
