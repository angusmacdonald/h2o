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
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2.engine.Database;
import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.manager.util.SystemTableMigrationState;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTable implements SystemTableRemote {

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
    public int addConnectionInformation(final DatabaseURL databaseURL, final DatabaseInstanceWrapper remoteDatabase) throws RemoteException, MovedException {

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
    public boolean addTableInformation(final TableManagerRemote tableManager, final TableInfo tableDetails, final Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException, MovedException {

        Diagnostic.trace("adding for table: " + tableDetails.getTableName());

        preMethodTest();

        try {
            return inMemory.addTableInformation(tableManager, tableDetails, replicaLocations) && persisted.addTableInformation(tableManager, tableDetails, replicaLocations);
        }
        catch (final SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean removeTableInformation(final TableInfo ti) throws RemoteException, MovedException {

        preMethodTest();
        final boolean result = inMemory.removeTableInformation(ti);
        persisted.removeTableInformation(ti);

        return result;
    }

    /******************************************************************
     **** Methods which only require checking in memory data structures.
     ******************************************************************/

    @Override
    public boolean exists(final TableInfo ti) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.exists(ti);
    }

    @Override
    public Set<String> getAllTablesInSchema(final String schemaName) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getAllTablesInSchema(schemaName);
    }

    @Override
    public int getNewTableSetNumber() throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getNewTableSetNumber();
    }

    @Override
    public TableManagerWrapper lookup(final TableInfo ti) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.lookup(ti);
    }

    @Override
    public void buildSystemTableState(final ISystemTable otherSystemTable) throws RemoteException, MovedException, SQLException {

        preMethodTest();
        inMemory.buildSystemTableState(otherSystemTable);

        persisted.buildSystemTableState(otherSystemTable);
    }

    @Override
    public void buildSystemTableState() throws RemoteException, MovedException, SQLException {

        preMethodTest();
        inMemory.buildSystemTableState(persisted);

    }

    @Override
    public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, MovedException, SQLException {

        return inMemory.getConnectionInformation();
    }

    @Override
    public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RemoteException, MovedException {

        return inMemory.getTableManagers();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTable#getReplicaLocations()
     */
    @Override
    public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations() throws RemoteException, MovedException, MovedException {

        return inMemory.getReplicaLocations();
    }

    @Override
    public void removeAllTableInformation() throws RemoteException, MovedException, MovedException, MovedException {

        preMethodTest();
        try {
            inMemory.removeAllTableInformation();
            persisted.removeAllTableInformation();
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DatabaseInstanceRemote getDatabaseInstance(final DatabaseURL databaseURL) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getDatabaseInstance(databaseURL);
    }

    @Override
    public Queue<DatabaseInstanceWrapper> getAvailableMachines(final ActionRequest typeOfRequest) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getAvailableMachines(typeOfRequest);
    }

    @Override
    public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getDatabaseInstances();
    }

    @Override
    public void removeConnectionInformation(final DatabaseInstanceRemote localDatabaseInstance) throws RemoteException, MovedException {

        preMethodTest();
        inMemory.removeConnectionInformation(localDatabaseInstance);
        persisted.removeConnectionInformation(localDatabaseInstance);
    }

    private void preMethodTest() throws RemoteException, MovedException {

        if (migrationState.shutdown) {
            throw new RemoteException(null);
        }
        else if (migrationState.hasMoved) { throw new MovedException(migrationState.movedLocation); }
        /*
         * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period), throw an execption.
         */
        if (migrationState.inMigration) {
            // If it hasn't moved, but is in the process of migration an
            // exception will be thrown.
            final long currentTimeOfMigration = System.currentTimeMillis() - migrationState.migrationTime;

            if (currentTimeOfMigration < MIGRATION_TIMEOUT) { throw new RemoteException("System Table is in the process of being moved."); }
            migrationState.inMigration = false; // Timeout request.
            migrationState.migrationTime = 0l;
        }
    }

    @Override
    public synchronized void prepareForMigration(final String newLocation) throws RemoteException, MovedException, MigrationException {

        preMethodTest();

        migrationState.movedLocation = newLocation;

        migrationState.inMigration = true;

        migrationState.migrationTime = System.currentTimeMillis();
    }

    @Override
    public void completeMigration() throws RemoteException, MovedException, MigrationException {

        if (!migrationState.inMigration) { // the migration process has timed
                                           // out.
            throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
        }

        migrationState.hasMoved = true;
        migrationState.inMigration = false;

        ((InMemorySystemTable) inMemory).removeTableManagerCheckerThread();
    }

    @Override
    public void checkConnection() throws RemoteException, MovedException {

        preMethodTest();
    }

    @Override
    public void changeTableManagerLocation(final TableManagerRemote stub, final TableInfo tableInfo) throws RemoteException, MovedException {

        preMethodTest();

        inMemory.changeTableManagerLocation(stub, tableInfo);
        persisted.changeTableManagerLocation(stub, tableInfo);
    }

    @Override
    public void shutdown(final boolean shutdown) throws RemoteException, MovedException {

        migrationState.shutdown = shutdown;
    }

    @Override
    public IChordRemoteReference getChordReference() throws RemoteException {

        return migrationState.location;
    }

    @Override
    public Set<TableManagerWrapper> getLocalDatabaseInstances(final DatabaseURL localMachineLocation) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.getLocalDatabaseInstances(localMachineLocation);
    }

    @Override
    public void addTableManagerStateReplica(final TableInfo table, final DatabaseURL replicaLocation, final DatabaseURL primaryLocation, final boolean active) throws RemoteException, MovedException {

        preMethodTest();
        inMemory.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
        persisted.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
    }

    @Override
    public void removeTableManagerStateReplica(final TableInfo table, final DatabaseURL replicaLocation) throws RemoteException, MovedException {

        preMethodTest();
        inMemory.removeTableManagerStateReplica(table, replicaLocation);
        persisted.removeTableManagerStateReplica(table, replicaLocation);
    }

    @Override
    public Map<TableInfo, DatabaseURL> getPrimaryLocations() throws RemoteException, MovedException {

        return inMemory.getPrimaryLocations();
    }

    @Override
    public TableManagerRemote recreateTableManager(final TableInfo table) throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.recreateTableManager(table);
    }

    @Override
    public boolean checkTableManagerAccessibility() throws RemoteException, MovedException {

        preMethodTest();
        return inMemory.checkTableManagerAccessibility();
    }
}
