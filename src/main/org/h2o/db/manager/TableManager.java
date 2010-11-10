/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2o.autonomic.decision.ranker.metric.CreateTableRequest;
import org.h2o.autonomic.framework.AutonomicAction;
import org.h2o.autonomic.framework.AutonomicController;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.util.Migratable;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.ILockingTable;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.query.locking.LockingTable;
import org.h2o.db.replication.ReplicaManager;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * <p>
 * The Table Manager represents a user table in H2O, and is responsible for storing information on replicas for that table, and handing out
 * locks to access those replicas.
 * </p>
 * 
 * <p>
 * There is one Table Manager for every user table in the system.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableManager extends PersistentManager implements TableManagerRemote, AutonomicController, Migratable {

    /**
     * Name of the schema used to store Table Manager tables.
     */
    private static final String SCHEMA = "H2O.";

    /**
     * Name of tables' table in Table Manager.
     */
    public static final String TABLES = "H2O_TM_TABLE";

    /**
     * Name of replicas' table in Table Manager.
     */
    public static final String REPLICAS = "H2O_TM_REPLICA";

    /**
     * Name of connections' table in Table Manager.
     */
    public static final String CONNECTIONS = "H2O_TM_CONNECTION";

    public static final String TABLEMANAGERSTATE = "H2O_TM_TABLEMANAGERS";

    /**
     * The name of the table that this Table Manager is responsible for.
     */
    private final String tableName;

    /**
     * Name of the schema in which the table is located.
     */
    private String schemaName;

    private ReplicaManager replicaManager;

    // /**
    // * Updates made asynchronously to a single table that haven't yet reached
    // other replicas.
    // *
    // * <p>Key: The number given to the update by the Table Manager.
    // * <p>Value: The SQL query for the update.
    // */
    // private Map<Integer, String> unPropagatedUpdates;
    // private Map<Integer, String> inProgressUpdates;

    /**
     * Stores locks held by various databases for accessing this table (all replicas).
     */
    private final ILockingTable lockingTable;

    private boolean shutdown = false;

    /*
     * MIGRATION RELATED CODE.
     */
    /**
     * If this System Table has been moved to another location (i.e. its state has been transferred to another machine and it is no longer
     * active) this field will not be null, and will note the new location of the System Table.
     */

    private String movedLocation = null;

    /**
     * Whether the System Table is in the process of being migrated. If this is true the System Table will be 'locked', unable to service
     * requests.
     */
    private boolean inMigration;

    /**
     * Whether the System Table has been moved to another location.
     */
    private boolean hasMoved = false;

    /**
     * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
     */
    private long migrationTime = 0l;

    /**
     * The timeout period for migrating the System Table.
     */
    private static final int MIGRATION_TIMEOUT = 10000;

    private final IChordRemoteReference location;

    private final String fullName;

    private final int relationReplicationFactor;

    private final TableInfo tableInfo;

    public TableManager(final TableInfo tableDetails, final Database database) throws Exception {

        super(database);

        final String dbName = database.getURL().sanitizedLocation();
        setMetaDataTableNames(getMetaTableName(dbName, TABLES), getMetaTableName(dbName, REPLICAS), getMetaTableName(dbName, CONNECTIONS), getMetaTableName(dbName, TABLEMANAGERSTATE));

        tableName = tableDetails.getTableName();

        schemaName = tableDetails.getSchemaName();

        if (schemaName == null || schemaName.equals("")) {
            schemaName = "PUBLIC";
        }

        fullName = schemaName + "." + tableName;
        tableInfo = tableDetails.getGenericTableInfo();

        replicaManager = new ReplicaManager();
        replicaManager.add(database.getLocalDatabaseInstanceInWrapper()); // the first replica will be created here.

        lockingTable = new LockingTable(schemaName + "." + tableName);

        location = database.getChordInterface().getLocalChordReference();

        relationReplicationFactor = Integer.parseInt(database.getDatabaseSettings().get("RELATION_REPLICATION_FACTOR"));

    }

    public static String getMetaTableName(final String databaseName, final String tablePostfix) {

        return SCHEMA + "H2O_" + databaseName + "_" + tablePostfix;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#addTableInformation(org.h2.h2o .util.DatabaseURL, org.h2.h2o.util.TableInfo)
     */
    @Override
    public boolean addTableInformation(final DatabaseURL tableManagerURL, final TableInfo tableDetails) throws RemoteException, MovedException, SQLException {

        final int result = super.addConnectionInformation(tableManagerURL, true);

        final boolean added = result != -1;
        if (!added) { return false; }

        return super.addTableInformation(tableManagerURL, tableDetails, true);

        /*
         * The System Table isn't contacted here, but in the Create Table class. This is because the Table isn't officially created until
         * the end of CreateTable.update().
         */
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#addReplicaInformation(org.h2.h2o .util.TableInfo)
     */
    @Override
    public void addReplicaInformation(final TableInfo tableDetails) throws RemoteException, MovedException, SQLException {

        preMethodTest();

        super.addConnectionInformation(tableDetails.getURL(), true);
        super.addReplicaInformation(tableDetails);
        replicaManager.add(getDatabaseInstance(tableDetails.getURL()));
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#removeReplicaInformation(org.h2 .h2o.util.TableInfo)
     */
    @Override
    public void removeReplicaInformation(final TableInfo ti) throws RemoteException, MovedException {

        super.removeReplicaInformation(ti);

        DatabaseInstanceRemote dbInstance = getDB().getDatabaseInstance(ti.getURL());
        if (dbInstance == null) {
            dbInstance = getDB().getDatabaseInstance(ti.getURL());
            if (dbInstance == null) {
                ErrorHandling.errorNoEvent("Couldn't remove replica location.");
            }
        }

        replicaManager.remove(new DatabaseInstanceWrapper(null, dbInstance, true));

    }

    @Override
    public boolean removeTableInformation() throws RemoteException, SQLException, MovedException {

        return removeTableInformation(getTableInfo(), true);
    }

    @Override
    public boolean removeTableInformation(final TableInfo tableInfo, final boolean removeReplicaInfo) {

        return super.removeTableInformation(getTableInfo(), removeReplicaInfo);
    }

    /**
     * Creates the set of tables used by the Table Manager.
     * 
     * @return Result of the update.
     * @throws SQLException
     */
    public static int createTableManagerTables(final Session session) throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Creating Table Manager tables.");

        final StringBuilder builder = new StringBuilder();

        final String databaseName = session.getDatabase().getURL().sanitizedLocation().toUpperCase();
        String sql = createSQL(getMetaTableName(databaseName, TableManager.TABLES), getMetaTableName(databaseName, TableManager.CONNECTIONS));

        builder.append(sql);
        builder.append("\n\nCREATE TABLE IF NOT EXISTS ");
        builder.append(getMetaTableName(databaseName, TableManager.REPLICAS));
        builder.append("(replica_id INTEGER NOT NULL auto_increment(1,1), table_id INTEGER NOT NULL, connection_id INTEGER NOT NULL, storage_type VARCHAR(255), active boolean NOT NULL, ");
        builder.append("table_set INT NOT NULL, PRIMARY KEY (replica_id), FOREIGN KEY (table_id) REFERENCES ");
        builder.append(getMetaTableName(databaseName, TableManager.TABLES));
        builder.append(" (table_id) ON DELETE CASCADE ,  FOREIGN KEY (connection_id) REFERENCES ");
        builder.append(getMetaTableName(databaseName, TableManager.CONNECTIONS));
        builder.append(" (connection_id));");
        sql += builder.toString();

        final Parser parser = new Parser(session, true);

        final Command query = parser.prepareCommand(sql);
        try {
            return query.update();
        }
        catch (final RemoteException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * Get the database instance at the specified database URL.
     * 
     * @param dbURL
     *            location of the database instance.
     * @return null if the instance wasn't found (including if it wasn't active).
     */
    private DatabaseInstanceWrapper getDatabaseInstance(final DatabaseURL dbURL) {

        final ISystemTable systemTable = getDB().getSystemTableReference().getSystemTable();

        DatabaseInstanceRemote dir = null;

        if (systemTable != null) {
            try {
                dir = systemTable.getDatabaseInstance(dbURL);
            }
            catch (final RemoteException e1) {
                e1.printStackTrace();
            }
            catch (final MovedException e1) {
                try {
                    getDB().getSystemTableReference().handleMovedException(e1);
                    dir = systemTable.getDatabaseInstance(dbURL);

                }
                catch (final Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (dir == null) {
            try {
                // The System Table doesn't contain a proper reference for the
                // remote database instance. Try and find one,
                // then update the System Table if successful.
                dir = getDB().getRemoteInterface().getDatabaseInstanceAt(dbURL);

                if (dir == null) {
                    ErrorHandling.errorNoEvent("DatabaseInstanceRemote wasn't found.");
                }
                else {

                    getDB().getSystemTable().addConnectionInformation(dbURL, new DatabaseInstanceWrapper(dbURL, dir, true));

                }

            }
            catch (final Exception e) {
                // e.printStackTrace();
            }
        }

        return new DatabaseInstanceWrapper(dbURL, dir, true);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.ITableManager#getQueryProxy(java.lang.String)
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getQueryProxy(org.h2.h2o.util. LockType, org.h2.h2o.comms.remote.DatabaseInstanceRemote)
     */
    @Override
    public synchronized TableProxy getQueryProxy(LockType lockRequested, final LockRequest lockRequest) throws RemoteException, SQLException, MovedException {

        preMethodTest();

        if (replicaManager.allReplicasSize() == 0 && !lockRequested.equals(LockType.CREATE)) { throw new SQLException("Illegal State. There must be at least one replica"); }

        int currentUpdateID = replicaManager.getCurrentUpdateID();
        boolean isDrop = false;

        if (lockRequested == LockType.DROP) {
            /*
             * If a table is dropped then created again with auto-commit turned off the update ID given here is higher than it is expected
             * to be on the create table operation. This just resets the update ID on the preceding drop. The LockType of DROP is not used
             * anywhere else, so the request is processed in the locking table as a write.
             */
            currentUpdateID = 0;
            isDrop = true;
            lockRequested = LockType.WRITE;
        }

        final LockType lockGranted = lockingTable.requestLock(lockRequested, lockRequest);

        final TableProxy qp = new TableProxy(lockGranted, tableInfo, selectReplicaLocations(lockRequested, lockRequest, isDrop), this, lockRequest, currentUpdateID, lockRequested);

        return qp;
    }

    /**
     * <p>
     * Selects a set of replica locations on which replicas will be created for a given table or schema.
     * 
     * <p>
     * This decision is currently based on the DESIRED_REPLICATION_FACTOR variable (if the query is a create), the SYNCHRONOUS_UPDATE
     * variable if the query is another form of update, and the database instance where the request was initiated.
     * 
     * @param primaryLocation
     *            The location of the primary copy - also the location of the Table Manager. This location will NOT be returned in the list
     *            of replica locations (because the primary copy already exists there).
     * @param lockType
     * @param isDrop
     * @param databaseInstanceRemote
     *            Requesting machine.
     * @return The set of database instances that should host a replica for the given table/schema. The return value will be NULL if no more
     *         replicas need to be created.
     */
    private Map<DatabaseInstanceWrapper, Integer> selectReplicaLocations(final LockType lockType, final LockRequest lockRequest, final boolean isDrop) {

        if (lockType == LockType.READ || lockType == LockType.NONE) { return replicaManager.getActiveReplicas(); }// else, a more informed decision is needed.

        /*
         * The set of machines onto which new replicas will be added.
         */
        final Map<DatabaseInstanceWrapper, Integer> newReplicaLocations = new HashMap<DatabaseInstanceWrapper, Integer>();

        /*
         * The set of all replica locations that could be involved in the query.
         */
        Queue<DatabaseInstanceWrapper> potentialReplicaLocations = null;

        if (lockType == LockType.CREATE) {
            /*
             * We know that the CREATE operation has been executed on the machine on which this Table Manager has been created, because it
             * is the create operation that initializes the Table Manager in the first place.
             */
            newReplicaLocations.put(lockRequest.getRequestLocation(), 0);
            if (relationReplicationFactor == 1) { return newReplicaLocations; // No more replicas are needed
            // currently.
            }

            try {
                // the update could be sent to any or all machines in the system.
                potentialReplicaLocations = getDB().getSystemTable().getAvailableMachines(new CreateTableRequest(0));
            }
            catch (final RemoteException e) {
                e.printStackTrace();
            }
            catch (final MovedException e) {
                e.printStackTrace();
            }

            int currentReplicationFactor = 1; // currently one copy of the table.

            /*
             * Loop through all potential replica locations, selecting enough to satisfy the system's replication fact. The location of the
             * primary copy cannot be re-used.
             */
            if (potentialReplicaLocations != null && potentialReplicaLocations.size() > 0) {

                for (final DatabaseInstanceWrapper dbInstance : potentialReplicaLocations) {
                    // This includes the location of the primary copy.
                    final Integer nullIfNoPrevious = newReplicaLocations.put(dbInstance, 0);
                    if (nullIfNoPrevious == null) {
                        currentReplicationFactor++;
                    }

                    /*
                     * Do we have enough replicas yet?
                     */
                    if (currentReplicationFactor == relationReplicationFactor) {
                        break;
                    }
                }

            }

            if (currentReplicationFactor < relationReplicationFactor) {
                // Couldn't replicate to enough machines.
                Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Insufficient number of machines available to reach a replication factor of " + relationReplicationFactor + ". The table will be replicated on " + currentReplicationFactor + " instances.");
            }

        }
        else if (lockType == LockType.WRITE) {
            Map<DatabaseInstanceWrapper, Integer> replicaLocations;

            if (isDrop) {
                /*
                 * If this is a drop table request we return a hashmap where the update IDs are all zero. This is due to a problem where
                 * AUTO COMMIT is off, and a new table is created after a table ahs been dropped. This results in the update ID being higher
                 * than expected unless we reset them on DROP.
                 */
                replicaLocations = new HashMap<DatabaseInstanceWrapper, Integer>();
                for (final Entry<DatabaseInstanceWrapper, Integer> entry : replicaManager.getAllReplicas().entrySet()) {
                    replicaLocations.put(entry.getKey(), 0);
                }
            }
            else {
                replicaLocations = replicaManager.getAllReplicas(); // The update could be sent to any or all machines holding the
                                                                    // given table.
            }

            return replicaLocations;
        }

        return newReplicaLocations;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.TableManagerRemote#getLocation()
     */
    @Override
    public DatabaseURL getLocation() throws RemoteException, MovedException {

        preMethodTest();

        return getDB().getURL();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getTableName()
     */
    @Override
    public String getTableName() {

        return tableName;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.TableManagerRemote#testAvailability()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#isAlive()
     */
    @Override
    public boolean isAlive() throws RemoteException, MovedException {

        if (shutdown) { return false; }

        preMethodTest();

        return true;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#releaseLock(org.h2.h2o.comms.remote .DatabaseInstanceRemote, java.util.Set, int)
     */
    @Override
    public void releaseLockAndUpdateReplicaState(final boolean commit, final LockRequest lockRequest, final Collection<CommitResult> committedQueries, final boolean asynchronousCommit) throws RemoteException, MovedException {

        /*
         * Release the locks.
         */
        final LockType lockType = lockingTable.peekAtLockGranted(lockRequest);

        /*
         * Update the set of 'active replicas' and their update IDs.
         */
        updateActiveReplicaSet(commit, committedQueries, asynchronousCommit, lockType);

        if (!asynchronousCommit) {
            lockingTable.releaseLock(lockRequest);
        }
    }

    /**
     * Update the set of 'active replicas' and their update IDs.
     * @param commit
     * @param committedQueries
     * @param asynchronousCommit
     * @param lockType
     */
    private void updateActiveReplicaSet(final boolean commit, final Collection<CommitResult> committedQueries, final boolean asynchronousCommit, final LockType lockType) {

        if (lockType == LockType.WRITE || asynchronousCommit) { // LockType.WRITE == LockType.CREATE in the locking table.
            final Set<DatabaseInstanceWrapper> changed = replicaManager.completeUpdate(commit, committedQueries, tableInfo, !asynchronousCommit);

            if (!asynchronousCommit && changed.size() < replicaManager.getActiveReplicas().size() && changed.size() > 1) {
                // This is the first part of a query. Some replicas will be made inactive.
                persistInactiveInformation(tableInfo, changed);
            }
            else {
                // This is the asynchronous part of the query. Some replicas will be made active.
                persistActiveInformation(tableInfo, changed);
            }

            printCurrentActiveReplicas();
        } // else: reads don't change the set of active replicas.
    }

    private void printCurrentActiveReplicas() {

        if (Diagnostic.getLevel().equals(DiagnosticLevel.INIT)) {

            final String databaseName = db.getURL().sanitizedLocation();
            final String sql = "SELECT LOCAL ONLY " + "connection_type, machine_name, db_location, connection_port, chord_port, " + getMetaTableName(databaseName, REPLICAS) + ".table_id, " + getMetaTableName(databaseName, REPLICAS) + ".connection_id FROM " + getMetaTableName(databaseName, REPLICAS)
                            + ", " + getMetaTableName(databaseName, TABLES) + ", " + getMetaTableName(databaseName, CONNECTIONS) + " WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND " + getMetaTableName(databaseName, REPLICAS) + ".active='true' AND "
                            + getMetaTableName(databaseName, TABLES) + ".table_id=" + getMetaTableName(databaseName, REPLICAS) + ".table_id AND " + getMetaTableName(databaseName, REPLICAS) + ".connection_id=" + getMetaTableName(databaseName, CONNECTIONS) + ".connection_id;";

            try {

                Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Current Active Replicas for table: " + tableName);
                final LocalResult rs = executeQuery(sql);

                while (rs.next()) {

                    final DatabaseURL dbURL = new DatabaseURL(rs.currentRow()[0].getString(), rs.currentRow()[1].getString(), rs.currentRow()[3].getInt(), rs.currentRow()[2].getString(), false, rs.currentRow()[4].getInt());

                    Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "\tLocation: " + dbURL + "; tableID = " + rs.currentRow()[5].getString() + "; connectionID = " + rs.currentRow()[6].getString());

                }
            }
            catch (final SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getTableInfo()
     */
    @Override
    public TableInfo getTableInfo() {

        return new TableInfo(tableName, schemaName, getDB().getURL());
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#shutdown()
     */
    @Override
    public void remove(final boolean dropCommand) {

        // Remove all persisted information
        removeTableInformation(getTableInfo(), true);

        shutdown = true;

        H2OEventBus.publish(new H2OEvent(db.getURL().getURL(), DatabaseStates.TABLE_MANAGER_SHUTDOWN));

        try {
            UnicastRemoteObject.unexportObject(this, true);
        }
        catch (final NoSuchObjectException e) {
        }
    }

    /*******************************************************
     * Methods implementing the Migrate interface.
     ***********************************************************/

    private void preMethodTest() throws RemoteException, MovedException {

        if (hasMoved) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Table Manager " + fullName + " has moved. Throwing MovedException.");
            throw new MovedException(movedLocation);
        }
        else if (shutdown) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Table Manager " + fullName + " has shutdown. An old reference was probably cached somewhere.");
            throw new MovedException(null);
        }
        /*
         * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period, throw an execption.
         */
        if (inMigration) {
            // If it hasn't moved, but is in the process of migration an
            // exception will be thrown.
            final long currentTimeOfMigration = System.currentTimeMillis() - migrationTime;

            if (currentTimeOfMigration > MIGRATION_TIMEOUT) {
                inMigration = false; // Timeout request.
                migrationTime = 0l;

                throw new RemoteException("Timeout exception. Migration took too long. Current time :" + currentTimeOfMigration + ", TIMEOUT time: " + MIGRATION_TIMEOUT);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.Migratable#checkConnection()
     */
    @Override
    public void checkConnection() throws RemoteException, MovedException {

        preMethodTest();

    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.Migratable#completeMigration()
     */
    @Override
    public void completeMigration() throws RemoteException, MovedException, MigrationException {

        if (!inMigration) { // the migration process has timed out.
            throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
        }

        hasMoved = true;
        inMigration = false;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.Migratable#prepareForMigration(java.lang.String)
     */
    @Override
    public void prepareForMigration(final String newLocation) throws RemoteException, MigrationException, MovedException {

        preMethodTest();

        movedLocation = newLocation;

        inMigration = true;

        migrationTime = System.currentTimeMillis();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#buildTableManagerState(org.h2. h2o.comms.remote.TableManagerRemote)
     */
    @Override
    public void buildTableManagerState(final TableManagerRemote otherTableManager) throws RemoteException, MovedException {

        preMethodTest();

        /*
         * Table name, schema name, and other infor are already obtained when the table manager instance is created.
         */

        /*
         * Obtain replica manager.
         */
        replicaManager = otherTableManager.getReplicaManager();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#getSchemaName()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getSchemaName()
     */
    @Override
    public String getSchemaName() throws RemoteException {

        return schemaName;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#getReplicaManager()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getReplicaManager()
     */
    @Override
    public ReplicaManager getReplicaManager() throws RemoteException, MovedException {

        preMethodTest();

        return replicaManager;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#getTableSet()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getTableSet()
     */
    @Override
    public int getTableSet() throws RemoteException {

        return 1; // TODO implement
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#getDatabaseURL()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getDatabaseURL()
     */
    @Override
    public DatabaseURL getDatabaseURL() throws RemoteException {

        return getDB().getURL();
    }

    /*******************************************************
     * Methods implementing the AutonomicController interface.
     ***********************************************************/

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.autonomic.AutonomicController#changeSetting(org.h2.h2o.autonomic .AutonomicAction)
     */
    @Override
    public boolean changeSetting(final AutonomicAction action) throws RemoteException {

        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.Migratable#shutdown(boolean)
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#shutdown(boolean)
     */
    @Override
    public void shutdown(final boolean shutdown) throws RemoteException, MovedException {

        this.shutdown = shutdown;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.Migratable#getChordReference()
     */
    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.TableManagerRemote2#getChordReference()
     */
    @Override
    public IChordRemoteReference getChordReference() throws RemoteException {

        return location;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#recreateReplicaManagerState()
     */
    @Override
    public void recreateReplicaManagerState(final String oldPrimaryDatabaseName) throws RemoteException, SQLException {

        final ReplicaManager rm = new ReplicaManager();

        /*
         * Get Replica information from persisted state.
         */

        final String oldTableRelation = getMetaTableName(oldPrimaryDatabaseName, TABLES);
        final String oldconnectionRelation = getMetaTableName(oldPrimaryDatabaseName, CONNECTIONS);
        final String oldReplicaRelation = getMetaTableName(oldPrimaryDatabaseName, REPLICAS);

        final String sql = "SELECT LOCAL ONLY connection_type, machine_name, db_location, connection_port, chord_port FROM " + oldReplicaRelation + ", " + oldTableRelation + ", " + oldconnectionRelation + " WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND "
                        + oldReplicaRelation + ".active='true' AND " + oldTableRelation + ".table_id=" + oldReplicaRelation + ".table_id AND " + oldconnectionRelation + ".connection_id=" + oldReplicaRelation + ".connection_id;";

        LocalResult rs = null;
        try {
            rs = executeQuery(sql);
        }
        catch (final SQLException e) {
            ErrorHandling.errorNoEvent(db.getURL() + ": Error replicating table manager state.");
            throw e;
        }

        final List<DatabaseInstanceWrapper> replicaLocations = new LinkedList<DatabaseInstanceWrapper>();
        while (rs.next()) {

            final DatabaseURL dbURL = new DatabaseURL(rs.currentRow()[0].getString(), rs.currentRow()[1].getString(), rs.currentRow()[3].getInt(), rs.currentRow()[2].getString(), false, rs.currentRow()[4].getInt());

            // Don't include the URL of the old instance unless it is still running.
            final DatabaseInstanceWrapper replicaLocation = getDatabaseInstance(dbURL);

            boolean alive = true;
            if (dbURL.sanitizedLocation().equals(oldPrimaryDatabaseName)) {
                try {
                    alive = replicaLocation.getDatabaseInstance().isAlive();
                }
                catch (final Exception e) {
                    alive = false;
                }
            }

            replicaLocation.setActive(alive); // even dead replicas must be recorded.
            replicaLocations.add(replicaLocation);

            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Active replica for " + tableName + " found on " + dbURL);

        }

        if (replicaLocations.size() == 0) { throw new SQLException("No replicas were listed for this table (" + fullName + "). An internal error has occured."); }

        rm.add(replicaLocations);

        replicaManager = rm;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#getNumberofReplicas()
     */
    @Override
    public int getNumberofReplicas() throws RemoteException {

        return replicaManager.getNumberOfReplicas();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.comms.remote.TableManagerRemote#persistToCompleteStartup()
     */
    @Override
    public void persistToCompleteStartup(final TableInfo tableInfo) throws RemoteException, StartupException {

        try {
            addTableInformation(getDB().getURL(), tableInfo);
        }
        catch (final MovedException e) {
            throw new StartupException("Newly created Table Manager throws a MovedException. This should never happen - serious internal error.");
        }
        catch (final SQLException e) {
            throw new StartupException("Failed to persist table manager meta-data to disk: " + e.getMessage());
        }

    }

    public void persistReplicaInformation() {

        for (final DatabaseInstanceWrapper dir : replicaManager.getActiveReplicas().keySet()) {
            final TableInfo ti = new TableInfo(getTableInfo());
            ti.setURL(dir.getURL());
            try {
                super.addConnectionInformation(ti.getURL(), true);
                super.addReplicaInformation(ti);
            }
            catch (final RemoteException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (final MovedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (final SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {

        return "TableManager [fullName=" + fullName + ", lockingTable=" + lockingTable + "]";
    }

}
