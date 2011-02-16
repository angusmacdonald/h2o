/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2o.db.manager.recovery;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.h2.engine.Database;
import org.h2o.db.DatabaseInstanceProxy;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.manager.SystemTable;
import org.h2o.db.manager.SystemTableReference;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.remote.IDatabaseRemote;
import org.h2o.db.wrappers.SystemTableWrapper;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.PrettyPrinter;

public class SystemTableFailureRecovery implements ISystemTableFailureRecovery {

    private final Database db;

    private final SystemTableReference stReference;

    private IDatabaseRemote remoteInterface;

    public SystemTableFailureRecovery(final Database db, final SystemTableReference systemTableReference) {

        this.db = db;
        stReference = systemTableReference;
        remoteInterface = db.getRemoteInterface();
    }

    @Override
    public synchronized SystemTableWrapper get() throws LocatorException, SystemTableAccessException {

        /*
         * 1. Get the location of the System Table (as the lookup instance currently knows it. 2. Contact the registry of that instance to
         * get a direct reference to the system table. If this registry (or the System Table) does not exist at this location any more an
         * error will be thrown. This happens when a query is made before maintenance mechanisms have kicked in. When this happens this node
         * should attempt to find a new location on which to re-instantiate a System Table. This replicates what is done in
         * ChordRemote.predecessorChangeEvent.
         */

        try {
            return tryToFindSystemTableViaLocator();
        }
        catch (final SQLException e) {
            //This just means that no System Table was active, not that it can't be found anywhere.
            ErrorHandling.errorNoEvent(db.getID() + ": Couldn't find active System Table at any of the locator sites. Will try to recreate System Table elsewhere.");
        }

        return reinstantiateSystemTable();
    }

    @Override
    public synchronized SystemTableWrapper find(final MovedException e) throws SQLException, RPCException {

        final String newLocation = e.getMessage();

        if (newLocation == null) { throw new SQLException("The System Table has been shutdown. It must be re-instantiated before another query can be answered."); }

        final DatabaseID systemTableLocationURL = DatabaseID.parseURL(newLocation);
        final IDatabaseInstanceRemote databaseInstance = lookForDatabaseInstanceAt(remoteInterface, systemTableLocationURL);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, db.getID() + ": This System Table reference is old. It has been moved to: " + newLocation);

        if (databaseInstance.isSystemTable()) {
            final SystemTableWrapper wrapper = new SystemTableWrapper(databaseInstance.getSystemTable(), databaseInstance.getURL());
            return wrapper;
        }
        throw new SQLException(db.getID() + ": Failed to find new location of System Table at " + systemTableLocationURL);
    }

    @Override
    public synchronized SystemTableWrapper restart(final boolean persistedSchemaTablesExist, final boolean recreateFromPersistedState, final ISystemTableMigratable oldSystemTable) throws SystemTableAccessException {

        if (recreateFromPersistedState) { return restartSystemTableFromPersistedState(persistedSchemaTablesExist); }
        return moveSystemTableToLocalMachine(oldSystemTable);
    }

    /*
     * ################################################ Local Recovery Methods... ################################################
     */

    /**
     * Recreate the System Table somewhere - the local machine if possible, but if not somewhere else which has persisted copies of the
     * System Table's state.
     * 
     * @param stReference
     * @param remoteInterface
     * @param db
     * @return
     * @throws LocatorException
     *             Thrown if the locator server could not be found.
     * @throws SystemTableAccessException
     *             Thrown if the System Table could not be restarted.
     */
    private SystemTableWrapper reinstantiateSystemTable() throws LocatorException, SystemTableAccessException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Beginning attempt to re-instantiate the System Table.");

        /*
         * There is no guarantee this node has a replica of the System Table state. Obtain the list of replicas from the locator server.
         * There are a number of cases: 1. This node holds a copy of System Table state. It can then apply to the locator server to become
         * the new System Table. 2. Another active node holds a copy of the System Table state. This node should be informed of the failure.
         * It can then apply to the locator server itself. 3. No active node has System Table state. Nothing can be done.
         */

        SystemTableWrapper newSystemTableWrapper = null;

        final List<String> stLocations = getActiveSystemTableLocationsFromLocator();

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Instances holding System Table state: " + PrettyPrinter.toString(stLocations));

        boolean localMachineHoldsSystemTableState = false;
        for (final String location : stLocations) {
            final DatabaseID url = DatabaseID.parseURL(location);
            localMachineHoldsSystemTableState = url.equals(db.getID());
        }

        if (localMachineHoldsSystemTableState) {
            // Re-instantiate the System Table on this node
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getID() + ": A copy of the System Table state exists locally (on " + db.getID() + "). It will be re-instantiated here.");
            final ISystemTableMigratable newSystemTable = stReference.migrateSystemTableToLocalInstance(true, true); // throws
            // SystemTableCreationException
            // if it fails.
            newSystemTableWrapper = new SystemTableWrapper(newSystemTable, db.getID());
        }
        else {

            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getID() + ": Attempting to find another machine which can re-instantiate the System Table (this machine does not hold replicated state.");

            /*
             * Try to find an active instance with System Table state.
             */

            newSystemTableWrapper = startSystemTableOnOneOfSpecifiedMachines(stLocations);
        }

        return newSystemTableWrapper;
    }

    /**
     * Get the list of locations with a current copy of System Table state. The System Table should be running on one of these machines. If
     * it isn't any one of these machines can be used to re-instantiate it.
     * 
     * @param remoteInterface
     * @return List of DatabaseURLs in String form.
     * @throws LocatorException
     */
    private List<String> getActiveSystemTableLocationsFromLocator() throws LocatorException {

        if (remoteInterface == null) {
            remoteInterface = db.getRemoteInterface();
        }

        final H2OLocatorInterface locatorInterface = remoteInterface.getLocatorInterface();

        if (locatorInterface == null) { throw new LocatorException("Failed to find locator servers."); }

        List<String> stLocations = null;

        try {
            stLocations = locatorInterface.getLocations();
        }
        catch (final IOException e) {
            throw new LocatorException("Failed to obtain a list of instances which hold System Table state: " + e.getMessage());
        }

        return stLocations;
    }

    /**
     * The System Table connection has been lost. Try to connect to the System Table lookup location and obtain a reference to the new
     * System Table.
     * 
     * @throws SQLException         Thrown if an active System Table could not be found.
     * @throws LocatorException     If can't connect to the locator server.
     */
    private SystemTableWrapper tryToFindSystemTableViaLocator() throws SQLException, LocatorException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getID() + ": Attempting to fix a broken System Table connection.");

        final List<String> databaseIntanceLocationsFromLocator = getActiveSystemTableLocationsFromLocator();

        IDatabaseInstanceRemote databaseInstance = null;

        for (final String possibleDatabaseIntanceLocation : databaseIntanceLocationsFromLocator) {
            try {

                //lookForDatabaseInstanceAt(remoteInterface, DatabaseID.parseURL(possibleDatabaseIntanceLocation));
                final DatabaseID possibleURL = DatabaseID.parseURL(possibleDatabaseIntanceLocation);
                databaseInstance = DatabaseInstanceProxy.getProxy(possibleURL);

                final boolean isSystemTable = databaseInstance.isSystemTable();

                if (isSystemTable) {
                    final SystemTableWrapper wrapper = new SystemTableWrapper(databaseInstance.getSystemTable(), databaseInstance.getURL());
                    return wrapper;
                }
            }
            catch (final Exception e) {
                Diagnostic.trace(DiagnosticLevel.FULL, "database not active: " + possibleDatabaseIntanceLocation);
                // May be thrown if database isn't active.
            }
        }

        throw new SQLException(db.getID() + ": Couldn't find active System Table. System table replicas exist at: " + PrettyPrinter.toString(databaseIntanceLocationsFromLocator));
    }

    /**
     * Start the System Table on one of the System Table locations given by the stLocations parameter.
     * 
     * @param stLocations
     *            Locations where the new system table could possibly be started.
     * @return Reference to the new System Table.
     * @throws SQLException
     * @throws SystemTableAccessException
     */
    private SystemTableWrapper startSystemTableOnOneOfSpecifiedMachines(final List<String> stLocations) throws SystemTableAccessException {

        for (final String systemTableLocation : stLocations) {
            final DatabaseID url = DatabaseID.parseURL(systemTableLocation);

            IDatabaseInstanceRemote databaseInstance = null;

            try {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempting to start System Table. Looking to connect to instance at " + url);

                databaseInstance = DatabaseInstanceProxy.getProxy(url);
                //todo, as a backup mechanism also do this: databaseInstance = lookForDatabaseInstanceAt(remoteInterface, url);
            }
            catch (final Exception e) {
                // May be thrown if database isn't active.
                continue;
            }

            /*
             * Attempt to recreate the System Table on this machine.
             */
            if (databaseInstance != null) {

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Connected to machine at " + url + ". Beginning attempt to recreate System Table.");

                ISystemTableMigratable systemTable = null;
                try {
                    systemTable = databaseInstance.recreateSystemTable(); // throws a SystemTableCreationException if it fails.
                }
                catch (final RPCException e) {
                    // May be thrown if database isn't active.
                    continue;
                }
                catch (final SQLException e) {
                    // Thrown if it failed to create the System Table.
                    e.printStackTrace();
                    continue; // try another machine.
                }

                return new SystemTableWrapper(systemTable, url);
            }
        }

        throw new SystemTableAccessException("Failed to create new System Table.");
    }

    private IDatabaseInstanceRemote lookForDatabaseInstanceAt(IDatabaseRemote iDatabaseRemote, final DatabaseID url) throws RPCException {

        if (iDatabaseRemote == null) {
            iDatabaseRemote = db.getRemoteInterface();
            remoteInterface = iDatabaseRemote;
        }

        IDatabaseInstanceRemote databaseInstance;
        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Looking for database instance at: " + url.getHostname() + ":" + url.getRMIPort());
        databaseInstance = iDatabaseRemote.getDatabaseInstanceAt(url);
        return databaseInstance;
    }

    /**
     * Migrates an active System Table to the local machine.
     * 
     * @param database
     * @param oldSystemTable
     * @return
     */
    private SystemTableWrapper moveSystemTableToLocalMachine(ISystemTableMigratable oldSystemTable) throws SystemTableAccessException {

        /*
         * CREATE A NEW System Table BY COPYING THE STATE OF THE CURRENT ACTIVE IN-MEMORY System Table.
         */

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate System Table.");

        ISystemTableMigratable newSystemTable = null;

        /*
         * Create a new System Table instance locally.
         */
        try {
            newSystemTable = new SystemTable(db, true);
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
            throw new SystemTableAccessException("Failed to create new in-memory System Table.");
        }

        db.startSystemTableServer(newSystemTable);

        /*
         * Stop the old, remote, manager from accepting any more requests.
         */
        try {
            oldSystemTable.prepareForMigration(db.getID().getURLwithRMIPort());
        }
        catch (final RPCException e) {
            e.printStackTrace();
        }
        catch (final MigrationException e) {
            ErrorHandling.exceptionError(e, "This System Table is already being migrated to another instance.");
            throw new SystemTableAccessException("This System Table is already being migrated to another instance.");
        }
        catch (final MovedException e) {

            try {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "The local instance (where the System Table is being migrated to) has an old System Table reference that must be updated.");
                stReference.handleMovedException(e);
            }
            catch (final SQLException e1) {
                //Failed to find new System Table
                e1.printStackTrace();
                throw new SystemTableAccessException("This System Table has already being migrated to another instance and that other instance is inaccessible");
            }

            try {
                oldSystemTable = stReference.getSystemTable();
                oldSystemTable.prepareForMigration(db.getID().getURLwithRMIPort());
            }
            catch (final RPCException e1) {
                e.printStackTrace();
            }
            catch (final MigrationException e1) {
                ErrorHandling.exceptionError(e, "This System Table is already being migrated to another instance.");
                throw new SystemTableAccessException("This System Table is already being migrated to another instance.");
            }
            catch (final MovedException e1) {
                ErrorHandling.exceptionError(e, "This System Table has already been migrated to another instance.");
                throw new SystemTableAccessException("This System Table is already being migrated to another instance.");
            }
        }

        /*
         * Build the System Table's state from that of the existing table.
         */
        try {
            newSystemTable.recreateSystemTable(oldSystemTable);
        }
        catch (final RPCException e) {
            ErrorHandling.exceptionError(e, "Failed to migrate System Table to new machine.");
            throw new SystemTableAccessException("Failed to migrate System Table to new machine.");
        }
        catch (final MovedException e) {
            ErrorHandling.exceptionError(e, "This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
            throw new SystemTableAccessException("This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
        }
        catch (final SQLException e) {
            ErrorHandling.exceptionError(e, "Couldn't create persisted tables as expected.");
            throw new SystemTableAccessException("Couldn't create persisted tables as expected.");
        }
        catch (final NullPointerException e) {
            // ErrorHandling.exceptionError(e,
            // "Failed to migrate System Table to new machine. Machine has already been shut down.");
        }

        /*
         * Shut down the old, remote, System Table. Redirect requests to new manager.
         */
        try {
            oldSystemTable.completeMigration();
        }
        catch (final RPCException e) {
            ErrorHandling.exceptionError(e, "Failed to complete migration.");
            throw new SystemTableAccessException("Failed to complete migration.");

        }
        catch (final MovedException e) {
            ErrorHandling.exceptionError(e, "This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
            throw new SystemTableAccessException("This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
        }
        catch (final MigrationException e) {
            ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
            throw new SystemTableAccessException("Migration process timed out. It took too long.");
        }
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "System Table officially migrated to " + db.getID().getDbLocation() + ".");

        final SystemTableWrapper wrapper = new SystemTableWrapper(newSystemTable, db.getID());
        return wrapper;
    }

    /**
     * Restarts the System Table on the local machine from persisted state at this machine.
     * 
     * @throws SystemTableAccessException
     */
    private SystemTableWrapper restartSystemTableFromPersistedState(final boolean persistedSchemaTablesExist) throws SystemTableAccessException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to re-instantiate System Table from persistent store.");

        ISystemTableMigratable newSystemTable = null;

        /*
         * INSTANTIATE A NEW System Table FROM PERSISTED STATE. This must be called if the previous System Table has failed.
         */
        if (!persistedSchemaTablesExist) {
            ErrorHandling.hardError("The system doesn't have a mechanism for recreating the state of the System Table from remote machines.");
        }

        try {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Creating new System Table instance");
            newSystemTable = new SystemTable(db, false); // false - don't overwrite saved persisted state.

            db.startSystemTableServer(newSystemTable);

        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
            throw new SystemTableAccessException("Failed to create new in-memory System Table.");
        }

        try {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Building state of new System Table");
            newSystemTable.recreateInMemorySystemTableFromLocalPersistedState();
            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, db.getID() + ": New System Table created.");
        }
        catch (final RPCException e) {
            e.printStackTrace();
            throw new SystemTableAccessException("Failed to contact some remote process when recreating System Table locally.");
        }
        catch (final MovedException e) {
            e.printStackTrace();
            throw new SystemTableAccessException("This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
        }
        catch (final SQLException e) {
            ErrorHandling.exceptionError(e, "Persisted state didn't exist on machine as expected.");
            throw new SystemTableAccessException("Persisted state didn't exist on machine as expected.");
        }

        final SystemTableWrapper wrapper = new SystemTableWrapper(newSystemTable, db.getID());

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Created new System Table at " + wrapper.getURL());

        return wrapper;
    }
}
