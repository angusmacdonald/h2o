package org.h2o.db.manager.interfaces;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public interface ISystemTable {

    /**
     * Find a reference for the Table Manager responsible for the given table.
     * 
     * @param tableName
     *            Name of the table whose Table Manager we are looking for.
     * @return A reference to the given table's Table Manager.
     * @throws RPCException
     */
    public TableManagerWrapper lookup(TableInfo ti) throws RPCException, MovedException;

    /**
     * Checks whether a Table Manager for the given table exists in the system. If it doesn't exist then it indicates that it is possible to
     * create a table with the given name.
     * 
     * @param tableName
     *            Name of the table being checked for.
     * @return True if the table exists in the system.
     * @throws RPCException
     */
    public boolean exists(TableInfo ti) throws RPCException, MovedException;

    /**
     * Confirm that the specified table has now been created, and provide a reference to the table's data manager.
     * 
     * @param tableManager
     *            Table Manager for the given table.
     * @param replicaLocations
     * @param session
     * @return True if this action was successful on the System Table; otherwise false.
     * @throws RPCException
     * @throws SQLException
     */
    public boolean addTableInformation(ITableManagerRemote tableManager, TableInfo tableDetails, Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException;

    /**
     * Remove Table Manager from the system. This is used when a table is being dropped completely from the database system.
     * 
     * <p>
     * If the tableName parameter is NULL the entire schema will be dropped.
     * 
     * @param tableName
     *            The table to be dropped.
     * @param schemaName
     *            the name of the schema where this table can be found.
     * @return true if the Table Manager was dropped successfully; otherwise false.
     * @throws RPCException
     */
    public boolean removeTableInformation(TableInfo ti) throws RPCException, MovedException;

    /**
     * Add information about a new database instance to the System Table.
     * 
     * @param databaseURL
     *            The name and location of the new database instance.
     * @throws SQLException
     */
    public int addConnectionInformation(DatabaseID databaseURL, DatabaseInstanceWrapper databaseInstanceWrapper) throws RPCException, MovedException, SQLException;

    /**
     * Get a new table set number from the System Table. Each number given is unique (i.e. the same number should not be given twice).
     * 
     * @return
     */
    public int getNewTableSetNumber() throws RPCException, MovedException;

    // /**
    // * Get the number of replicas that exist for a particular table.
    // * @param tableName The name of the table.
    // * @param schemaName The schema which this table is in. NULL is
    // acceptable, and
    // * used to indicate the default 'PUBLIC' schema.
    // * @return
    // */
    // public int getNumberofReplicas(String tableName, String schemaName)
    // throws RPCException, MovedException;

    // /**
    // * Add details of a new replica at the specified location.
    // * @param ti
    // */
    // public void addReplicaInformation(TableInfo ti) throws RPCException,
    // MovedException, SQLException;

    /**
     * Returns an array of all the tables in a given database schema.
     * 
     * @param schemaName
     *            the name of the schema in question.
     * @return Array of table names from the specified schema.
     */
    public Set<String> getAllTablesInSchema(String schemaName) throws RPCException, MovedException;

    /**
     * Build the state of this System Table object by replicating the state of another schema manager.
     * 
     * @param otherSystemTable
     *            The System Table whose state is to be taken.
     * @throws RPCException
     * @throws SQLException
     */
    public void recreateSystemTable(ISystemTable otherSystemTable) throws RPCException, MovedException, SQLException;

    /**
     * Build the state of this System Table object by replicating the state of the local persistent System Table.
     * 
     * @throws RPCException
     * @throws SQLException
     */
    void recreateInMemorySystemTableFromLocalPersistedState() throws RPCException, MovedException, SQLException;

    /**
     * Returns a set of all the databases connected in the system.
     * 
     * @throws SQLException
     */
    public Map<DatabaseID, DatabaseInstanceWrapper> getConnectionInformation() throws RPCException, MovedException, SQLException;

    /**
     * Returns a map of all Table Managers in the system.
     */
    public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RPCException, MovedException;

    /**
     * Returns a map of all replicas in the database system. Key is the fully qualified name of the table, value is the set of replica
     * locations.
     */
    public Map<TableInfo, Set<DatabaseID>> getReplicaLocations() throws RPCException, MovedException;

    /**
     * Remove all references to Table Managers and replicas. Used to shutdown a System Table.
     * 
     * @throws RPCException
     */
    public void removeAllTableInformation() throws RPCException, MovedException;

    /**
     * Get a remote reference to a database instance at the specified URL.
     * 
     * @param databaseURL
     *            URL of the database reference.
     * @return Remote reference to the database instance.
     */
    public IDatabaseInstanceRemote getDatabaseInstance(DatabaseID databaseURL) throws RPCException, MovedException;

    /**
     * Get remote references to every database instance in the database system.
     * 
     * @return The set of all databases in the system.
     */
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException;

    /**
     * Remove connection information for a database instance.
     * 
     * @param localDatabaseInstance
     */
    public void removeConnectionInformation(IDatabaseInstanceRemote localDatabaseInstance) throws RPCException, MovedException;

    /**
     * Get the Table Manager instances stored local to the given location.
     * 
     * @param localMachineLocation
     * @return
     */
    public Set<TableManagerWrapper> getLocalTableManagers(DatabaseID localMachineLocation) throws RPCException, MovedException;

    /**
     * Called when the location of the Table Manager is to be changed.
     * 
     * @param stub
     */
    public void changeTableManagerLocation(ITableManagerRemote stub, TableInfo tableInfo) throws RPCException, MovedException;

    /**
     * Add a new location where a given table managers state has been replicated.
     * 
     * @param table
     *            The table whose manager has just replicated its state.
     * @param replicaLocation
     *            Where the state has been replicated.
     */
    public void addTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation, DatabaseID primaryLocation, boolean active) throws RPCException, MovedException;

    public Map<TableInfo, DatabaseID> getPrimaryLocations() throws RPCException, MovedException;

    /**
     * Add a location where a given table managers state was replicated.
     * 
     * @param table
     *            The table whose manager has removed/lost a replica of its state.
     * @param replicaLocation
     *            Where the state was replicated.
     */
    public void removeTableManagerStateReplica(TableInfo table, DatabaseID replicaLocation) throws RPCException, MovedException;

    /**
     * Recreate the table manager for the the table specified by the parameter if it has failed.
     * 
     * @param table
     * @return
     * @throws RPCException
     * @throws MovedException
     */
    public ITableManagerRemote recreateTableManager(TableInfo table) throws RPCException, MovedException;

    /**
     * Instructs the System Table to check that all Table Managers are currently active.
     * 
     * @return
     * @throws MovedException
     * @throws RPCException
     */
    public boolean checkTableManagerAccessibility() throws RPCException, MovedException;

    /**
     * Inform the System table that a database instance has possibly failed.
     * 
     * <p>The system table will try to contact the instance itself, and if it can't, it removes
     * it from the membership set.
     * @param predecessorURL the instance that is suspected of failure.
     */
    public void suspectInstanceOfFailure(DatabaseID predecessorURL) throws RPCException, MovedException;

    public int getCurrentSystemTableReplication() throws RPCException, MovedException;

    public Set<DatabaseInstanceWrapper> getNoReplicateInstances();
}
