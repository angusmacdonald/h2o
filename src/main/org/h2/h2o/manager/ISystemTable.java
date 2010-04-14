package org.h2.h2o.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISystemTable extends Remote {

	/**
	 * Find a reference for the Table Manager responsible for the given table.
	 * 
	 * @param tableName	Name of the table whose Table Manager we are looking for.
	 * @return A reference to the given table's Table Manager.
	 * @throws RemoteException
	 */
	public TableManagerRemote lookup(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Checks whether a Table Manager for the given table exists in the system. If it
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
	 * @param tableManager	Table Manager for the given table.
	 * @param session 
	 * @return True if this action was successful on the System Table; otherwise false.
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails) throws RemoteException, MovedException, SQLException;


	/**
	 * Remove a single replica from the database system (so there will be other replicas left).
	 * @param ti			Information on the replicas location.
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Remove Table Manager from the system. This is used when a table is being dropped completely
	 * from the database system.
	 * 
	 * <p>If the tableName parameter is NULL the entire schema will be dropped.
	 * 
	 * @param tableName	The table to be dropped.
	 * @param schemaName the name of the schema where this table can be found.
	 * @return	true if the Table Manager was dropped successfully; otherwise false.
	 * @throws RemoteException
	 */
	public boolean removeTableInformation(TableInfo ti) throws RemoteException, MovedException;

	/**
	 * Add information about a new database instance to the System Table.
	 * @param databaseURL	The name and location of the new database instance.
	 * @throws SQLException 
	 */
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceWrapper databaseInstanceWrapper) throws RemoteException, MovedException, SQLException;

	/**
	 * Get a new table set number from the System Table. Each number given is unique (i.e. the same number should not be given twice).
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
	public void addReplicaInformation(TableInfo ti) throws RemoteException, MovedException, SQLException;

	/**
	 * Returns an array of all the tables in a given database schema.
	 * @param schemaName the name of the schema in question.
	 * @return Array of table names from the specified schema.
	 */
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException, MovedException;
	
	/**
	 * Build the state of this System Table object by replicating the state of another schema
	 * manager.
	 * @param otherSystemTable	The System Table whose state is to be taken.
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public void buildSystemTableState(ISystemTable otherSystemTable) throws RemoteException, MovedException, SQLException;


	/**
	 * Build the state of this System Table object by replicating the state of the local
	 * persistent System Table.
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	void buildSystemTableState() throws RemoteException, MovedException, SQLException;
	
	/**
	 * Returns a set of all the databases connected in the system.
	 * @throws SQLException 
	 */
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, MovedException, SQLException;

	/**
	 * Returns a map of all Table Managers in the system.
	 */
	public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RemoteException, MovedException;

	/**
	 * Returns a map of all replicas in the database system. Key is the fully
	 * qualified name of the table, value is the set of replica locations.
	 */
	public Map<String, Set<TableInfo>> getReplicaLocations() throws RemoteException, MovedException;

	/**
	 * Remove all references to Table Managers and replicas. Used to shutdown a System Table.
	 * @throws RemoteException 
	 */
	public void removeAllTableInformation() throws RemoteException, MovedException;

	/**
	 * Specify the remote location of a database instance where System Table state is to be replicated.
	 * @param databaseReference
	 * @throws RemoteException
	 * @throws MovedException 
	 */
	public void addStateReplicaLocation(DatabaseInstanceRemote databaseReference) throws RemoteException, MovedException;

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
	public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RemoteException, MovedException;

	/**
	 * @param localDatabaseInstance
	 */
	public void removeConnectionInformation(DatabaseInstanceRemote localDatabaseInstance) throws RemoteException, MovedException;

	/**
	 * Called when the location of the Table Manager is to be changed.
	 * @param stub
	 */
	public void changeTableManagerLocation(TableManagerRemote stub, TableInfo tableInfo) throws RemoteException, MovedException;


	/**
	 * Get the Table Manager instances stored local to the given location.
	 * @param localMachineLocation
	 * @return
	 */
	public Set<TableManagerWrapper> getLocalDatabaseInstances(DatabaseURL localMachineLocation) throws RemoteException, MovedException;

}
