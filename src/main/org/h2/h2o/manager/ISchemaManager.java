package org.h2.h2o.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISchemaManager extends Remote {

	/**
	 * Find a reference for the data manager responsible for the given table.
	 * 
	 * @param tableName	Name of the table whose data manager we are looking for.
	 * @return A reference to the given table's data manager.
	 * @throws RemoteException
	 */
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Checks whether a data manager for the given table exists in the system. If it
	 * doesn't exist then it indicates that it is possible to create a table with the given name.
	 * 
	 * @param tableName	Name of the table being checked for.
	 * @return True if the table exists in the system.
	 * @throws RemoteException
	 */
	public boolean exists(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Confirm that the specified table has now been created, and provide a reference to the table's data
	 * manager.
	 * 
	 * @param dataManager	Data manager for the given table.
	 * @param session 
	 * @return True if this action was successful on the schema manager; otherwise false.
	 * @throws RemoteException
	 */
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails) throws RemoteException, MovedException;


	/**
	 * Remove a single replica from the database system (so there will be other replicas left).
	 * @param ti			Information on the replicas location.
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Remove data manager from the system. This is used when a table is being dropped completely
	 * from the database system.
	 * 
	 * <p>If the tableName parameter is NULL the entire schema will be dropped.
	 * 
	 * @param tableName	The table to be dropped.
	 * @param schemaName the name of the schema where this table can be found.
	 * @return	true if the data manager was dropped successfully; otherwise false.
	 * @throws RemoteException
	 */
	public boolean removeTableInformation(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Add information about a new database instance to the schema manager.
	 * @param databaseURL	The name and location of the new database instance.
	 */
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceRemote databaseInstanceRemote) throws RemoteException, MovedException;

	/**
	 * Get a new table set number from the schema manager. Each number given is unique (i.e. the same number should not be given twice).
	 * 
	 * @return
	 */
	public int getNewTableSetNumber() throws RemoteException, MovedException;

	/**
	 * Get the number of replicas that exist for a particular table.
	 * @param tableName	The name of the table.
	 * @param schemaName	The schema which this table is in. NULL is acceptable, and
	 * used to indicate the default 'PUBLIC' schema.
	 * @return
	 */
	public int getNumberofReplicas(String tableName, String schemaName) throws RemoteException, MovedException;

	/**
	 * Add details of a new replica at the specified location.
	 * @param ti
	 */
	public void addReplicaInformation(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Returns an array of all the tables in a given database schema.
	 * @param schemaName the name of the schema in question.
	 * @return Array of table names from the specified schema.
	 */
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException, MovedException;
	
	/**
	 * Build the state of this schema manager object by replicating the state of another schema
	 * manager.
	 * @param otherSchemaManager	The schema manager whose state is to be taken.
	 * @throws RemoteException
	 */
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager) throws RemoteException, MovedException;


	/**
	 * Build the state of this schema manager object by replicating the state of the local
	 * persistent schema manager.
	 * @throws RemoteException
	 */
	void buildSchemaManagerState() throws RemoteException, MovedException;
	
	/**
	 * Returns a set of all the databases connected in the system.
	 */
	public Map<DatabaseURL, DatabaseInstanceRemote> getConnectionInformation() throws RemoteException, MovedException;

	/**
	 * Returns a map of all data managers in the system.
	 */
	public Map<TableInfo, DataManagerRemote> getDataManagers() throws RemoteException, MovedException;

	/**
	 * Returns a map of all replicas in the database system. Key is the fully
	 * qualified name of the table, value is the set of replica locations.
	 */
	public Map<String, Set<TableInfo>> getReplicaLocations() throws RemoteException, MovedException;

	/**
	 * Remove all references to data managers and replicas. Used to shutdown a schema manager.
	 * @throws RemoteException 
	 */
	public void removeAllTableInformation() throws RemoteException, MovedException;

	/**
	 * Specify the remote location of a database instance where schema manager state is to be replicated.
	 * @param databaseReference
	 * @throws RemoteException
	 * @throws MovedException 
	 */
	public void addSchemaManagerDataLocation(DatabaseInstanceRemote databaseReference) throws RemoteException, MovedException;

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
	public void completeSchemaManagerMigration() throws RemoteException, MovedException, MigrationException ;


	/**
	 * Get a remote reference to a database instance at the specified URL.
	 * @param databaseURL	URL of the database reference.
	 * @return Remote reference to the database instance.
	 */
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) throws RemoteException, MovedException;

	/**
	 * Get remote references to every database instance in the database system.
	 * @return	The set of all databases in the system.
	 */
	public Set<DatabaseInstanceRemote> getDatabaseInstances() throws RemoteException, MovedException;

	/**
	 * @param localDatabaseInstance
	 */
	public void removeConnectionInformation(DatabaseInstanceRemote localDatabaseInstance) throws RemoteException, MovedException;


}
