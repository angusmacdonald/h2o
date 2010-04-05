package org.h2.h2o.remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.DataManager;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * <p>Classes implementing this interface must manage connections to the schema manager, and to
 * other database instances.
 *  
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDatabaseRemote {

	public DatabaseURL connectToDatabaseSystem(Session systemSession);

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
	public int getLocalRMIPort();

	/**
	 * Remove references to remote objects in preparation for the shutdown of the database system.
	 */
	public void shutdown();

	/**
	 * Get the location of the local database instance, including the port the database
	 * is running on (JDBC) and the port the databases RMI connection is running on.
	 * @return Address of the local database instance.
	 */
	public DatabaseURL getLocalMachineLocation();

	/**
	 * Find a reference to a remote data manager, not from the schema manager but from
	 * first principles (lookup via RMI registry) - this is required when rebuilding the schema manager,
	 * or using a persistent schema manager in the lookup.
	 * @param dbURL
	 */
	public DataManagerRemote refindDataManagerReference(TableInfo ti, DatabaseURL dbURL);

	/**
	 * @param schemaManagerRef
	 */
	void bindSchemaManagerReference(SchemaManagerReference schemaManagerRef);

	/**
	 * 
	 */
	public void exportConnectionObject();

	/**
	 * @throws RemoteException 
	 * 
	 */
	public IChordRemoteReference lookupSchemaManagerNodeLocation() throws RemoteException;

	/**
	 * @return
	 */
	public IChordRemoteReference getLocalChordReference();

	/**
	 * @return
	 */
	public IChordRemoteReference getSchemaManagerLookupLocation();

	/**
	 * @param lookupLocation
	 * @throws RemoteException 
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(IChordRemoteReference lookupLocation) throws RemoteException;

	/**
	 * @return
	 */
	public ChordInterface getChordInterface();

	/**
	 * @param hostname
	 * @param port
	 * @throws RemoteException 
	 * @throws NotBoundException 
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(String hostname, int port) throws RemoteException, NotBoundException;
}