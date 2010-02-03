package org.h2.h2o.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DataManagerRemote;
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
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException;

	/**
	 * Checks whether a data manager for the given table exists in the system. If it
	 * doesn't exist then it indicates that it is possible to create a table with the given name.
	 * 
	 * @param tableName	Name of the table being checked for.
	 * @return True if the table exists in the system.
	 * @throws RemoteException
	 */
	public boolean exists(TableInfo ti) throws RemoteException;

	/**
	 * Confirm that the specified table has now been created, and provide a reference to the table's data
	 * manager.
	 * 
	 * @param dataManager	Data manager for the given table.
	 * @param session 
	 * @return True if this action was successful on the schema manager; otherwise false.
	 * @throws RemoteException
	 */
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails) throws RemoteException;


	/**
	 * Remove a single replica from the database system (so there will be other replicas left).
	 * @param ti			Information on the replicas location.
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException;

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
	public boolean removeTableInformation(TableInfo ti) throws RemoteException;

	/**
	 * Add information about a new database instance to the schema manager.
	 * @param databaseURL	The name and location of the new database instance.
	 */
	public int addConnectionInformation(DatabaseURL databaseURL) throws RemoteException;

	/**
	 * Get a new table set number from the schema manager. Each number given is unique (i.e. the same number should not be given twice).
	 * 
	 * @return
	 */
	public int getNewTableSetNumber() throws RemoteException;

	/**
	 * Get the number of replicas that exist for a particular table.
	 * @param tableName	The name of the table.
	 * @param schemaName	The schema which this table is in. NULL is acceptable, and
	 * used to indicate the default 'PUBLIC' schema.
	 * @return
	 */
	public int getNumberofReplicas(String tableName, String schemaName) throws RemoteException;

	/**
	 * Add details of a new replica at the specified location.
	 * @param ti
	 */
	public void addReplicaInformation(TableInfo ti) throws RemoteException;

	/**
	 * Returns an array of all the tables in a given database schema.
	 * @param schemaName the name of the schema in question.
	 * @return Array of table names from the specified schema.
	 */
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException;
	
	/**
	 * Build the state of this schema manager object by replicating the state of another schema
	 * manager.
	 * @param otherSchemaManager	The schema manager whose state is to be taken.
	 * @throws RemoteException
	 */
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager) throws RemoteException;


	/**
	 * Build the state of this schema manager object by replicating the state of the local
	 * persistent schema manager.
	 * @throws RemoteException
	 */
	void buildSchemaManagerState() throws RemoteException;
	
	/**
	 * Returns a set of all the databases connected in the system.
	 */
	public Set<DatabaseURL> getConnectionInformation() throws RemoteException;

	/**
	 * Returns a map of all data managers in the system.
	 */
	public Map<TableInfo, DataManagerRemote> getDataManagers() throws RemoteException;

	/**
	 * Returns a map of all replicas in the database system. Key is the fully
	 * qualified name of the table, value is the set of replica locations.
	 */
	public Map<String, Set<TableInfo>> getReplicaLocations() throws RemoteException;

	/**
	 * Remove all references to data managers and replicas. Used to shutdown a schema manager.
	 * @throws RemoteException 
	 */
	public void removeAllTableInformation() throws RemoteException;


}
