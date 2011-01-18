/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager;

import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2.engine.Database;
import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableRemote;
import org.h2o.db.manager.util.SystemTableMigrationState;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTable implements ISystemTableRemote {

    /**
     * Interface to the in-memory state of the System Table.
     */
    private final ISystemTable inMemory;

    /**
     * Interface to the persisted state of this System Table. This object interacts with the database to store the state of the System Table
     * on disk.
     */
    private final ISystemTable persisted;

    /**
     * Fields related to the migration functionality of the System Table.
     */
    private final SystemTableMigrationState migrationState;

    /**
     * The timeout period for migrating the System Table.
     */
    private static final int MIGRATION_TIMEOUT = 10000;

    public SystemTable(final Database db, final boolean createTables) throws Exception {

        inMemory = new InMemorySystemTable(db);
        persisted = new PersistentSystemTable(db, createTables);

        migrationState = new SystemTableMigrationState(db.getChordInterface().getLocalChordReference());
    }

    /******************************************************************
     **** Methods which require both in memory and persisted data structures to be updated.
     ******************************************************************/

    @Override
    public int addConnectionInformation(final DatabaseID databaseURL, final DatabaseInstanceWrapper remoteDatabase) throws RPCException, MovedException {

        preMethodTest();

        try {
            inMemory.addConnectionInformation(databaseURL, remoteDatabase);
            return persisted.addConnectionInformation(databaseURL, remoteDatabase);
        }
        catch (final SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public synchronized boolean addTableInformation(final ITableManagerRemote tableManager, final TableInfo tableDetails, final Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException {

        preMethodTest();

        try {
            return persisted.addTableInformation(tableManager, tableDetails, replicaLocations) && inMemory.addTableInformation(tableManager, tableDetails, replicaLocations);
        }
        catch (final SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean removeTableInformation(final TableInfo ti) throws RPCException, MovedException {

        preMethodTest();
        final boolean result = inMemory.removeTableInformation(ti);
        persisted.removeTableInformation(ti);

        return result;
    }

    /******************************************************************
     **** Methods which only require checking in memory data structures.
     ******************************************************************/

    @Override
    public boolean exists(final TableInfo ti) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.exists(ti);
    }

    @Override
    public Set<String> getAllTablesInSchema(final String schemaName) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getAllTablesInSchema(schemaName);
    }

    @Override
    public int getNewTableSetNumber() throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getNewTableSetNumber();
    }

    @Override
    public TableManagerWrapper lookup(final TableInfo ti) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.lookup(ti);
    }

    @Override
    public void buildSystemTableState(final ISystemTable otherSystemTable) throws RPCException, MovedException, SQLException {

        preMethodTest();
        inMemory.buildSystemTableState(otherSystemTable);

        persisted.buildSystemTableState(otherSystemTable);
    }

    @Override
    public void buildSystemTableState() throws RPCException, MovedException, SQLException {

        preMethodTest();
        inMemory.buildSystemTableState(persisted);

    }

    @Override
    public Map<DatabaseID, DatabaseInstanceWrapper> getConnectionInformation() throws RPCException, MovedException, SQLException {

        return inMemory.getConnectionInformation();
    }

    @Override
    public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RPCException, MovedException {

        return inMemory.getTableManagers();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTable#getReplicaLocations()
     */
    @Override
    public Map<TableInfo, Set<DatabaseID>> getReplicaLocations() throws RPCException, MovedException, MovedException {

        return inMemory.getReplicaLocations();
    }

    @Override
    public void removeAllTableInformation() throws RPCException, MovedException, MovedException, MovedException {

        preMethodTest();
        try {
            inMemory.removeAllTableInformation();
            persisted.removeAllTableInformation();
        }
        catch (final RPCException e) {
            e.printStackTrace();
        }
    }

    @Override
    public IDatabaseInstanceRemote getDatabaseInstance(final DatabaseID databaseURL) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getDatabaseInstance(databaseURL);
    }

    @Override
    public Queue<DatabaseInstanceWrapper> getAvailableMachines(final ActionRequest typeOfRequest) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getAvailableMachines(typeOfRequest);
    }

    @Override
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getDatabaseInstances();
    }

    @Override
    public void removeConnectionInformation(final IDatabaseInstanceRemote localDatabaseInstance) throws RPCException, MovedException {

        preMethodTest();
        inMemory.removeConnectionInformation(localDatabaseInstance);
        persisted.removeConnectionInformation(localDatabaseInstance);
    }

    private void preMethodTest() throws RPCException, MovedException {

        if (migrationState.shutdown) {
            throw new RPCException("");
        }
        else if (migrationState.hasMoved) { throw new MovedException(migrationState.movedLocation); }
        /*
         * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period), throw an execption.
         */
        if (migrationState.inMigration) {
            // If it hasn't moved, but is in the process of migration an
            // exception will be thrown.
            final long currentTimeOfMigration = System.currentTimeMillis() - migrationState.migrationTime;

            if (currentTimeOfMigration < MIGRATION_TIMEOUT) { throw new RPCException("System Table is in the process of being moved."); }
            migrationState.inMigration = false; // Timeout request.
            migrationState.migrationTime = 0l;
        }
    }

    @Override
    public synchronized void prepareForMigration(final String newLocation) throws RPCException, MovedException, MigrationException {

        preMethodTest();

        migrationState.movedLocation = newLocation;

        migrationState.inMigration = true;

        migrationState.migrationTime = System.currentTimeMillis();
    }

    @Override
    public void completeMigration() throws RPCException, MovedException, MigrationException {

        if (!migrationState.inMigration) { // the migration process has timed
                                           // out.
            throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
        }

        migrationState.hasMoved = true;
        migrationState.inMigration = false;

        ((InMemorySystemTable) inMemory).removeTableManagerCheckerThread();
    }

    @Override
    public void checkConnection() throws RPCException, MovedException {

        preMethodTest();
    }

    @Override
    public void changeTableManagerLocation(final ITableManagerRemote stub, final TableInfo tableInfo) throws RPCException, MovedException {

        preMethodTest();

        inMemory.changeTableManagerLocation(stub, tableInfo);
        persisted.changeTableManagerLocation(stub, tableInfo);
    }

    @Override
    public void shutdown(final boolean shutdown) throws RPCException, MovedException {

        migrationState.shutdown = shutdown;
    }

    @Override
    public IChordRemoteReference getChordReference() throws RPCException {

        return migrationState.location;
    }

    @Override
    public Set<TableManagerWrapper> getLocalDatabaseInstances(final DatabaseID localMachineLocation) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.getLocalDatabaseInstances(localMachineLocation);
    }

    @Override
    public void addTableManagerStateReplica(final TableInfo table, final DatabaseID replicaLocation, final DatabaseID primaryLocation, final boolean active) throws RPCException, MovedException {

        preMethodTest();
        inMemory.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
        persisted.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
    }

    @Override
    public void removeTableManagerStateReplica(final TableInfo table, final DatabaseID replicaLocation) throws RPCException, MovedException {

        preMethodTest();
        inMemory.removeTableManagerStateReplica(table, replicaLocation);
        persisted.removeTableManagerStateReplica(table, replicaLocation);
    }

    @Override
    public Map<TableInfo, DatabaseID> getPrimaryLocations() throws RPCException, MovedException {

        return inMemory.getPrimaryLocations();
    }

    @Override
    public ITableManagerRemote recreateTableManager(final TableInfo table) throws RPCException, MovedException {

        preMethodTest();
        return inMemory.recreateTableManager(table);
    }

    @Override
    public boolean checkTableManagerAccessibility() throws RPCException, MovedException {

        preMethodTest();
        return inMemory.checkTableManagerAccessibility();
    }
}
