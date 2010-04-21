package org.h2.h2o.remote;

import java.rmi.RemoteException;

import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.util.DatabaseURL;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * <p>Classes implementing this interface must manage connections to the System Table, and to
 * other database instances.
 *  
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDatabaseRemote {

	public DatabaseURL connectToDatabaseSystem(Session systemSession) throws StartupException;

	/**
	 * Get the remote reference of the local database instance.
	 * 
	 * <p>This is used as an identity when locking tables for a particular query.
	 * @return Remote reference of the local database.
	 */
	public DatabaseInstanceRemote getLocalDatabaseInstance();

	/**
	 * Returns the port on which the local database instance is running its RMI server.
	 * @return
	 */
	public int getRmiPort();

	/**
	 * Remove references to remote objects in preparation for the shutdown of the database system.
	 */
	public void shutdown();


	/**
	 * True if this chord node is in the process of shutting down.
	 */
	public boolean inShutdown();

	
	/**
	 * Get the location of the local database instance, including the port the database
	 * is running on (JDBC) and the port the databases RMI connection is running on.
	 * @return Address of the local database instance.
	 */
	public DatabaseURL getLocalMachineLocation();
	
	/**
	 * Export the System Table contained within this SystemTableReference via the UnicastRemoteObject class
	 * to allow it to be accessed remotely.
	 * @param systemTableRef	Local wrapper class for the System Table.
	 */
	public void exportSystemTable(ISystemTableReference systemTableRef);

	/**
	 * Find the database instance located at the location given. The chord reference parameter is used
	 * to get the hostname and port of that chord nodes RMI registry. This registry should contain a reference
	 * to the local database instance.
	 * @param lookupLocation	The hostname and port of this reference are used to find the local RMI registry.
	 * @throws RemoteException 	Thrown if there is a problem accessing the RMI registry.
	 * @return Database instance remote proxy for the database at the given location.
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(IChordRemoteReference lookupLocation) throws RemoteException;

	/**
	 * Find the database instance located at the location given. The parameter is used
	 * to get the hostname and RMI port of that chord nodes RMI registry. This registry should contain a reference
	 * to the local database instance.
	 * @param databaseURL The hostname and RMI port of this reference are used to find the local RMI registry.
	 * @return Database instance remote proxy for the database at the given location.
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(DatabaseURL databaseURL)  throws RemoteException;
	

	
}