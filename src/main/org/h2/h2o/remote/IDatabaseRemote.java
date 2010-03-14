package org.h2.h2o.remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

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
	 * Register a new data manager with the database system.
	 * @param dm	The data manager to be registered.
	 */
	public void registerDataManager(DataManager dm);

	/**
	 * Find a data manager for the given table in the database system.
	 * @param tableName	the table whose manager is to be found.
	 * @return	Remote reference to the data manager in question.
	 * @throws SQLException 
	 */
	public DataManagerRemote lookupDataManager(String tableName)
			throws SQLException;

	/**
	 * Remove a data manager from the schema manager.
	 * @param tableName		Data manager to be removed.
	 * @param removeLocalOnly	Whether it is only the local copy of a data manager's state which is to be removed.
	 */
	public void removeDataManager(String tableName, boolean removeLocalOnly);

	/**
	 * Get a remote reference to a database instance at the specified URL.
	 * @param databaseURL	URL of the database reference.
	 * @return Remote reference to the database instance.
	 */
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL);

	/**
	 * Get remote references to every database instance in the database system.
	 * @return	The set of all databases in the system.
	 */
	public Set<DatabaseInstanceRemote> getDatabaseInstances();

	/**
	 * Get the remote reference of the local database instance.
	 * 
	 * <p>This is used as an identity when locking tables for a particular query.
	 * @return Remote reference of the local database.
	 */
	public DatabaseInstanceRemote getLocalDatabaseInstance();

	/**
	 * Remove the local database instance from the schema manager and any other registries.
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 */
	public void removeLocalDatabaseInstance() throws RemoteException,
			NotBoundException;


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

}