/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.result.LocalResult;
import org.h2.value.Value;
import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.DefaultSettings;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.remote.IDatabaseRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PersistentSystemTable extends PersistentManager implements ISystemTable {

    /**
     * Name of the schema used to store System Table tables.
     */
    private static final String SCHEMA = "H2O.";

    /**
     * Name of tables' table in System Table.
     */
    public static final String TABLES = SCHEMA + "H2O_TABLE";

    /**
     * Name of connections' table in System Table.
     */
    public static final String CONNECTIONS = SCHEMA + "H2O_CONNECTION";

    /**
     * Name of the table which stores the location of table manager state replicas.
     */
    public static final String TABLEMANAGERSTATE = SCHEMA + "H2O_TABLEMANAGER_STATE";;

    /**
     * The database username used to communicate with System Table tables.
     */
    public static final String USERNAME = DefaultSettings.getString("PersistentSystemTable.DEFAULT_USERNAME");

    /**
     * The database password used to communicate with System Table tables.
     */
    public static final String PASSWORD = DefaultSettings.getString("PersistentSystemTable.DEFAULT_PASSWORD");

    public PersistentSystemTable(final Database db, final boolean createTables) throws Exception {

        super(db, TABLES, null, CONNECTIONS, TABLEMANAGERSTATE, true);

        if (createTables) {
            /*
             * Create a new set of schema tables locally.
             */
            try {
                String sql = createSQL(TABLES, CONNECTIONS);
                sql += "\n\nCREATE TABLE IF NOT EXISTS " + TABLEMANAGERSTATE + "(" + "table_id INTEGER NOT NULL, " + "connection_id INTEGER NOT NULL, " + "primary_location_connection_id INTEGER NOT NULL, " + "active BOOLEAN, " + "FOREIGN KEY (table_id) REFERENCES " + TABLES + " (table_id) ON DELETE CASCADE , " + " FOREIGN KEY (connection_id) REFERENCES " + CONNECTIONS + " (connection_id)); "; //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$ //$NON-NLS-9$ //$NON-NLS-10$

                final boolean success = getNewQueryParser();

                if (!success) { throw new StartupException("Database has already been shutdown."); }
                sqlQuery = queryParser.prepareCommand(sql);

                sqlQuery.update();
                sqlQuery.close();

            }
            catch (final SQLException e) {
                e.printStackTrace();
                throw new Exception("Couldn't create manager state tables.");
            }
        }

        super.updateLocatorFiles(true);
    }

    /**
     * Get the names of all the tables in a given schema.
     * 
     * @param schemaName
     * @return
     */
    @Override
    public Set<String> getAllTablesInSchema(final String schemaName) {

        final String sql = "SELECT tablename FROM " + TABLES + " WHERE schemaname='" + schemaName + "';"; //$NON-NLS-3$

        LocalResult result = null;

        try {
            sqlQuery = getParser().prepareCommand(sql);

            result = sqlQuery.executeQueryLocal(0);

            final Set<String> tableNames = new HashSet<String>();
            while (result.next()) {
                tableNames.add(result.currentRow()[0].getString());
            }

            return tableNames;

        }
        catch (final SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Returns the next available tableSet number. Not implemented in PersistentSystemTable.
     */
    @Override
    public int getNewTableSetNumber() {

        return 1;
    }

    @Override
    public boolean exists(final TableInfo ti) throws RemoteException {

        try {
            return isTableListed(ti);
        }
        catch (final SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public TableManagerWrapper lookup(final TableInfo ti) throws RemoteException {

        return null;
    }

    @Override
    public void buildSystemTableState(final ISystemTable otherSystemTable) throws RemoteException {

        /*
         * Persist the state of the given System Table reference to disk.
         */

        try {
            /*
             * Obtain references to connected machines.
             */
            Map<DatabaseURL, DatabaseInstanceWrapper> databasesInSystem = null;
            try {
                databasesInSystem = otherSystemTable.getConnectionInformation();
            }
            catch (final SQLException e) {
                e.printStackTrace();
            }

            if (databasesInSystem != null) {
                for (final Entry<DatabaseURL, DatabaseInstanceWrapper> databaseEntry : databasesInSystem.entrySet()) {
                    try {
                        addConnectionInformation(databaseEntry.getKey(), databaseEntry.getValue());
                    }
                    catch (final SQLException e) {
                        e.printStackTrace();
                    }
                }
            }

            /*
             * Obtain references to Table Managers.
             */

            final Map<TableInfo, TableManagerWrapper> tableManagers = otherSystemTable.getTableManagers();

            for (final Entry<TableInfo, TableManagerWrapper> dmEntry : tableManagers.entrySet()) {
                try {
                    super.addTableInformation(dmEntry.getValue().getTableManager().getDatabaseURL(), dmEntry.getKey(), false);
                }
                catch (final SQLException e) {
                    e.printStackTrace();
                }
            }

            /*
             * Get table manager replica locations
             */

            final Map<TableInfo, Set<DatabaseURL>> replicaLocations = otherSystemTable.getReplicaLocations();
            final Map<TableInfo, DatabaseURL> primaryLocations = otherSystemTable.getPrimaryLocations();

            for (final Entry<TableInfo, Set<DatabaseURL>> databaseEntry : replicaLocations.entrySet()) {
                for (final DatabaseURL tableInfo : databaseEntry.getValue()) {
                    addTableManagerStateReplica(databaseEntry.getKey(), tableInfo, primaryLocations.get(databaseEntry.getKey()), false);
                }
            }
        }
        catch (final MovedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, SQLException {

        return getConnectionInformation("SELECT * FROM " + CONNECTIONS + ";");
    }

    private Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation(final String query) throws RemoteException, SQLException {

        final Map<DatabaseURL, DatabaseInstanceWrapper> databaseLocations = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();

        final String sql = query;

        LocalResult result = null;

        try {
            sqlQuery = getParser().prepareCommand(sql);

            result = sqlQuery.executeQueryLocal(0);

            while (result.next()) {
                /*
                 * sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + "connection_id INT NOT NULL auto_increment," +
                 * "connection_type VARCHAR(5), " + "machine_name VARCHAR(255)," + "db_location VARCHAR(255)," +
                 * "connection_port INT NOT NULL, " + "rmi_port INT NOT NULL, " + "PRIMARY KEY (connection_id) );";
                 */
                final Value[] row = result.currentRow();
                final String connectionType = row[1].getString();
                final String hostName = row[2].getString();
                final String dbLocation = row[3].getString();
                final int dbPort = row[4].getInt();
                final int rmiPort = row[5].getInt();
                final DatabaseURL dbURL = new DatabaseURL(connectionType, hostName, dbPort, dbLocation, false);
                dbURL.setRMIPort(rmiPort);
                databaseLocations.put(dbURL, null);
            }

        }
        catch (final SQLException e) {
            e.printStackTrace();
        }

        return databaseLocations;

        /*
         * Parse connection information for every database. Does this include the RMI port?
         */
    }

    private DatabaseURL getDatabaseURL(final int replicaConnectionID) {

        Map<DatabaseURL, DatabaseInstanceWrapper> connectionInformation;
        try {
            connectionInformation = getConnectionInformation("SELECT * FROM " + CONNECTIONS + " WHERE connection_id=" + replicaConnectionID + ";"); //$NON-NLS-3$

            assert connectionInformation.size() <= 1 : "There shouldn't be multiple databases with the same connection ID";

            for (final DatabaseURL dbURL : connectionInformation.keySet()) {
                return dbURL; //return the only database URL with the specified connection ID.
            }
        }
        catch (final Exception e) {
            ErrorHandling.exceptionErrorNoEvent(e, "Failed to get the database URL of a database with the connection ID: " + replicaConnectionID);
        }

        return null;
    }

    @Override
    public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RemoteException {

        final Map<TableInfo, TableManagerWrapper> tableManagers = new HashMap<TableInfo, TableManagerWrapper>();

        final IDatabaseRemote remoteInterface = getDB().getRemoteInterface();

        /*
         * Parse the query resultset to find the primary location of every table.
         */
        final String sql = "SELECT db_location, connection_type, machine_name, connection_port, tablename, schemaname, chord_port FROM " + CONNECTIONS + ", " + TABLES + " WHERE " + CONNECTIONS + ".connection_id = " + TABLES + ".manager_location;";

        // SELECT db_location, connection_type, machine_name, connection_port,
        // tablename, schemaname, chord_port FROM H2O.H2O_REPLICA,
        // H2O.H2O_CONNECTION, H2O.H2O_TABLE
        // WHERE H2O.H2O_TABLE.table_id=H2O.H2O_REPLICA.table_id AND
        // H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND
        // primary_copy = true;

        LocalResult result = null;

        try {
            sqlQuery = getParser().prepareCommand(sql);

            result = sqlQuery.executeQueryLocal(0);

            while (result != null && result.next()) {
                final Value[] row = result.currentRow();

                final String dbLocation = row[0].getString();
                final String connectionType = row[1].getString();
                final String machineName = row[2].getString();
                final String connectionPort = row[3].getString();

                final String tableName = row[4].getString();
                final String schemaName = row[5].getString();
                final int chord_port = row[6].getInt();

                final DatabaseURL dbURL = new DatabaseURL(connectionType, machineName, Integer.parseInt(connectionPort), dbLocation, false, chord_port);
                final TableInfo ti = new TableInfo(tableName, schemaName);

                /*
                 * Perform lookups to get remote references to every Table Manager.
                 */
                DatabaseInstanceRemote dir = null;

                try {
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finding database instance at : " + dbURL);

                    dir = remoteInterface.getDatabaseInstanceAt(dbURL);
                }
                catch (final RemoteException e) {
                    // Will happen if its no longer active.
                }

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found database instance at : " + dbURL);

                if (dir != null) {
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finding table manager reference.");
                    TableManagerRemote tmReference = null;
                    try {
                        tmReference = dir.findTableManagerReference(ti, true);
                    }
                    catch (final RemoteException e1) {//thrown if dir is not accessible.
                        ErrorHandling.errorNoEvent("Failed to find Table Manager reference for " + ti + " when recreating System Table state.");
                    }
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found table manager reference.");

                    final TableManagerWrapper dmw = new TableManagerWrapper(ti, tmReference, dbURL);
                    tableManagers.put(ti, dmw);
                }
                else {
                    final TableManagerWrapper dmw = new TableManagerWrapper(ti, null, dbURL);
                    tableManagers.put(ti, dmw);
                }

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Created new Table Manager");
            }
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
        }

        return tableManagers;
    }

    @Override
    public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations() throws RemoteException {

        /*
         * Parse the schema tables to obtain the required amount of table information.
         */

        final String sql = "SELECT connection_id, tablename, schemaname    FROM " + TABLEMANAGERSTATE + ", " + TABLES + " WHERE " + TABLES + ".table_id" + "=" + TABLEMANAGERSTATE + ".table_id;"; //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

        LocalResult result = null;

        final Map<TableInfo, Set<DatabaseURL>> replicaLocations = new HashMap<TableInfo, Set<DatabaseURL>>();

        try {
            sqlQuery = getParser().prepareCommand(sql);

            result = sqlQuery.executeQueryLocal(0);

            while (result != null && result.next()) {
                final Value[] row = result.currentRow();

                final int replicaConnectionID = row[0].getInt();

                final String tableName = row[1].getString();
                final String schemaName = row[2].getString();

                final TableInfo ti = new TableInfo(tableName, schemaName);
                final DatabaseURL replicaURL = getDatabaseURL(replicaConnectionID);
                // DatabaseURL primaryURL =
                // getDatabaseURL(primaryManagerConnectionID);

                /*
                 * Add this replica to the set of replica locations for this table.
                 */
                Set<DatabaseURL> specificTableReplicaLocations = replicaLocations.get(ti);
                if (specificTableReplicaLocations == null) {
                    specificTableReplicaLocations = new HashSet<DatabaseURL>();
                }
                specificTableReplicaLocations.add(replicaURL);
                replicaLocations.put(ti, specificTableReplicaLocations);
            }
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
        }

        return replicaLocations;
    }

    @Override
    public Map<TableInfo, DatabaseURL> getPrimaryLocations() throws RemoteException, MovedException {

        /*
         * Parse the schema tables to obtain the required amount of table information.
         */

        final String sql = "SELECT primary_location_connection_id, tablename, schemaname    FROM " + TABLEMANAGERSTATE + ", " + TABLES + " WHERE " + TABLES + ".table_id" + "=" + TABLEMANAGERSTATE + ".table_id;"; //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$

        LocalResult result = null;

        final Map<TableInfo, DatabaseURL> primaryLocations = new HashMap<TableInfo, DatabaseURL>();

        try {
            sqlQuery = getParser().prepareCommand(sql);

            result = sqlQuery.executeQueryLocal(0);

            while (result != null && result.next()) {
                final Value[] row = result.currentRow();

                final int primaryManagerConnectionID = row[0].getInt();
                final String tableName = row[1].getString();
                final String schemaName = row[2].getString();

                final TableInfo ti = new TableInfo(tableName, schemaName);
                final DatabaseURL primaryURL = getDatabaseURL(primaryManagerConnectionID);

                /*
                 * Add this replica to the set of replica locations for this table.
                 */

                primaryLocations.put(ti, primaryURL);
            }
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
        }

        return primaryLocations;
    }

    @Override
    public void buildSystemTableState() throws RemoteException {

        // TODO Auto-generated method stub
    }

    @Override
    public void removeAllTableInformation() throws RemoteException, MovedException {

        removeTableInformation(null);
    }

    @Override
    public DatabaseInstanceRemote getDatabaseInstance(final DatabaseURL databaseURL) throws RemoteException, MovedException {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RemoteException, MovedException {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void changeTableManagerLocation(final TableManagerRemote locationOfManager, final TableInfo tableInfo) {

        super.changeTableManagerLocation(tableInfo);
    }

    @Override
    public boolean addTableInformation(final TableManagerRemote tableManager, final TableInfo tableDetails, final Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException, MovedException, SQLException {

        Diagnostic.trace("adding for table: " + tableDetails.getTableName());
        final boolean added = super.addTableInformation(tableManager.getDatabaseURL(), tableDetails, false);

        if (added) {
            final int connectionID = getConnectionID(tableDetails.getURL());
            addTableManagerReplicaInformationOnCreateTable(getTableID(tableDetails), connectionID, true, replicaLocations);
        }

        Diagnostic.trace("successful: " + added);
        return added;
    }

    @Override
    public int addConnectionInformation(final DatabaseURL databaseURL, final DatabaseInstanceWrapper databaseInstanceWrapper) throws RemoteException, MovedException, SQLException {

        return super.addConnectionInformation(databaseURL, databaseInstanceWrapper.isActive());
    }

    @Override
    public Set<TableManagerWrapper> getLocalDatabaseInstances(final DatabaseURL localMachineLocation) throws RemoteException, MovedException {

        final int connectionID = getConnectionID(localMachineLocation);

        assert connectionID != -1;

        final String sql = "SELECT tablename, schemaname FROM " + TABLES + "  WHERE manager_location= " + connectionID + ";";

        final Set<TableManagerWrapper> localTables = new HashSet<TableManagerWrapper>();
        try {
            final LocalResult rs = executeQuery(sql);

            while (rs.next()) {
                final TableInfo tableInfo = new TableInfo(rs.currentRow()[0].getString(), rs.currentRow()[1].getString());
                final TableManagerWrapper dmw = new TableManagerWrapper(tableInfo, null, null);
                localTables.add(dmw);
            }
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }

        return localTables;
    }

    @Override
    public void addTableManagerStateReplica(final TableInfo table, final DatabaseURL replicaLocation, final DatabaseURL primaryLocation, final boolean active) throws RemoteException, MovedException {

        try {
            addTableManagerReplicaInformation(getTableID(table), getConnectionID(replicaLocation), getConnectionID(primaryLocation), active);
        }
        catch (final SQLException e) {
            throw new RemoteException(e.getMessage());
        }
    }

    @Override
    public void removeTableManagerStateReplica(final TableInfo table, final DatabaseURL replicaLocation) throws RemoteException, MovedException {

        try {
            removeTableManagerReplicaInformation(getTableID(table), getConnectionID(replicaLocation));
        }
        catch (final SQLException e) {
            throw new RemoteException(e.getMessage());
        }
    }

    @Override
    protected DatabaseURL getLocation() throws RemoteException {

        return getDB().getURL();
    }

    @Override
    protected TableInfo getTableInfo() {

        return null;
    }

    @Override
    public boolean removeTableInformation(final TableInfo ti) throws RemoteException, MovedException {

        return removeTableInformation(ti, false);
    }

    @Override
    public TableManagerRemote recreateTableManager(final TableInfo table) throws RemoteException, MovedException {

        return null;
        // Done by in-memory system table.
    }

    @Override
    public boolean checkTableManagerAccessibility() {

        // Done by in-memory system table.
        return false;
    }

    @Override
    public Queue<DatabaseInstanceWrapper> getAvailableMachines(final ActionRequest typeOfRequest) {

        // TODO Auto-generated method stub
        return null;
    }
}
