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

package org.h2o.db.manager;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.table.ReplicaSet;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.recovery.LocatorException;
import org.h2o.db.manager.recovery.SystemTableAccessException;
import org.h2o.db.manager.recovery.SystemTableFailureRecovery;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.SystemTableWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.p2p.util.SHA1KeyFactory;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * Encapsulates SystemTable references, containing state on whether the reference is local or remote, whether the lookup is local or remote,
 * and other relevant information. This class manages operations on the System Table, such as migration between database instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableReference implements ISystemTableReference {

    /**
     * Name under which the System Table is located in the local RMI registry.
     */
    public static final String SCHEMA_MANAGER = "SCHEMA_MANAGER";

    /*
     * System Table STATE.
     */

    private final Map<TableInfo, ITableManagerRemote> cachedTableManagerReferences = new HashMap<TableInfo, ITableManagerRemote>();

    private final Map<TableInfo, TableManager> localTableManagers = new HashMap<TableInfo, TableManager>();

    /**
     * Whether the System Table is running on this node.
     */
    private boolean isLocal = false;

    /**
     * Whether the System Table lookup on Chord resolves to this machines keyspace.
     */
    private boolean inKeyRange = false;

    private IChordRemoteReference systemTableNode;

    /*
     * CHORD-RELATED.
     */
    /**
     * Reference to the remote chord node which is responsible for ensuring the System Table is running. This node is not necessarily the
     * actual location of the System Table.
     */
    private IChordRemoteReference lookupLocation;

    /**
     * Key factory used to create keys for System Table lookup and to search for specific machines.
     */
    private static SHA1KeyFactory keyFactory = new SHA1KeyFactory();

    /**
     * The key of the System Table. This must be used in lookup operations to find the current location of the schema manager reference.
     */
    public final static IKey systemTableKey = keyFactory.generateKey("systemTable");

    /*
     * GENERAL DATABASE.
     */
    /**
     * Reference to the local database instance. This is needed to get the local database URL, and to instantiate new System Table objects.
     */
    private final Database db;

    private final SystemTableFailureRecovery systemTableRecovery;

    private SystemTableWrapper systemTableWrapper = new SystemTableWrapper();

    /**
     * When a new object is created with this constructor the System Table reference may not exist, so only the database object is required.
     * 
     * @param db
     */
    public SystemTableReference(final Database db) {

        this.db = db;
        systemTableRecovery = new SystemTableFailureRecovery(db, this);
    }

    @Override
    public ISystemTableMigratable getSystemTable() {

        return getSystemTable(false);
    }

    @Override
    public ISystemTableMigratable getSystemTable(final boolean inShutdown) {

        boolean foundSystemTable = false;

        try {

            if (systemTableWrapper.getSystemTable() == null) {
                systemTableWrapper.setSystemTable(findSystemTable());
            }

            systemTableWrapper.getSystemTable().checkConnection();

            foundSystemTable = true;
        }
        catch (final MovedException e) {
            if (!inShutdown) {
                try {
                    handleMovedException(e);
                }
                catch (final SQLException e1) {
                    ErrorHandling.errorNoEvent("Error trying to handle a MovedException.");
                }
            }
        }
        catch (final Exception e) {

            /*
             * SQLException when: findSystemTable() has failed to find the System Table instances registry. This indicates that the system
             * table instance has failed, so we should try to recreate the System Table somewhere else.
             */
            e.printStackTrace();
            ErrorHandling.errorNoEvent(db.getID() + ": The current System Table reference points to an inactive instance. " + "H2O will attempt to find an active System Table.");

            try {
                systemTableWrapper = systemTableRecovery.get();
                foundSystemTable = true; // would throw an exception if it didn't.
            }
            catch (final LocatorException e1) {
                ErrorHandling.errorNoEvent("Couldn't find any locator servers when looking for the System Table: " + e1.getMessage());
            }
            catch (final SystemTableAccessException e1) {
                ErrorHandling.errorNoEvent("Tried to recreate the System Table (through recovery mechanisms) and failed to find a suitable instance.");
            }
        }

        if (foundSystemTable && systemTableWrapper.getSystemTable() != null) {
            try {
                systemTableNode = systemTableWrapper.getSystemTable().getChordReference();
            }
            catch (final RPCException e) {
                e.printStackTrace();
                ErrorHandling.errorNoEvent("Failed to obtain the new System Table's chord reference.");
            }
        }

        return systemTableWrapper.getSystemTable();
    }

    @Override
    public DatabaseID getSystemTableURL() {

        return systemTableWrapper.getURL();
    }

    @Override
    public boolean isSystemTableLocal() {

        return isLocal;
    }

    @Override
    public ISystemTableMigratable findSystemTable() throws SQLException {

        try {
            systemTableWrapper = systemTableRecovery.get();
            return systemTableWrapper.getSystemTable();
        }
        catch (final LocatorException e) {

            throw new SQLException("Couldn't find locator server(s).");
        }
        catch (final SystemTableAccessException e) {

            throw new SQLException("Couldn't create System Table.");
        }
    }

    @Override
    public void setSystemTableURL(final DatabaseID newSMLocation) {

        if (newSMLocation.equals(db.getID())) {
            isLocal = true;
        }

        systemTableWrapper.setURL(newSMLocation);
    }

    @Override
    public void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseID databaseURL) {

        systemTableNode = systemTableLocation;
        systemTableWrapper.setURL(databaseURL);
    }

    @Override
    public void setSystemTable(final ISystemTableMigratable systemTable) {

        systemTableWrapper.setSystemTable(systemTable);
    }

    @Override
    public boolean isConnectedToSM() {

        return systemTableWrapper.getSystemTable() != null;
    }

    @Override
    public void setInKeyRange(final boolean inKeyRange) {

        this.inKeyRange = inKeyRange;
    }

    @Override
    public boolean isInKeyRange() {

        return inKeyRange;
    }

    @Override
    public ISystemTableMigratable migrateSystemTableToLocalInstance(final boolean persistedSchemaTablesExist, final boolean recreateFromPersistedState) throws SystemTableAccessException {

        systemTableWrapper = systemTableRecovery.restart(persistedSchemaTablesExist, recreateFromPersistedState, systemTableWrapper.getSystemTable());

        db.getMetaDataReplicaManager().replicateMetaDataIfPossible(this, true); // replicate system table state.

        /*
         * Make the new System Table remotely accessible.
         */
        isLocal = systemTableWrapper.getURL().equals(db.getID());

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new System Table on " + db.getID().getDbLocation() + ".");
        H2OEventBus.publish(new H2OEvent(db.getID().getURL(), DatabaseStates.SYSTEM_TABLE_MIGRATION));
        return systemTableWrapper.getSystemTable();
    }

    @Override
    public void migrateSystemTableToLocalInstance() throws SystemTableAccessException {

        boolean persistedSchemaTablesExist = false;

        for (final ReplicaSet rs : db.getAllTables()) {
            if (rs.getTableName().contains("H2O_") && rs.getLocalCopy() != null) {
                persistedSchemaTablesExist = true;
                break;
            }
        }

        migrateSystemTableToLocalInstance(persistedSchemaTablesExist, false);
    }

    @Override
    public void setLookupLocation(final IChordRemoteReference proxy) {

        lookupLocation = proxy;
    }

    @Override
    public IChordRemoteReference getLookupLocation() {

        return lookupLocation;
    }

    @Override
    public ITableManagerRemote lookup(final String tableName, final boolean useCache) throws SQLException {

        return lookup(new TableInfo(tableName), useCache);
    }

    @Override
    public ITableManagerRemote lookup(final TableInfo tableInfo, final boolean useCache) throws SQLException {

        return lookup(tableInfo, useCache, false);
    }

    @Override
    public ITableManagerRemote lookup(final TableInfo tableInfo, final boolean useCache, final boolean searchOnlyCache) throws SQLException {

        if (tableInfo == null) { return null; }

        /*
         * The Table Manager may exist in one of two local caches, or it may have to be found via the Schema Manager. The caches are tested
         * first, in the following order: CHECK ONE: Look in cache of Local Table Managers. CHECK TWO: The Table Manager is not local. Look
         * in the cache of Remote Table Manager References. CHECK THREE: The Table Manager proxy is not known. Contact the System Table for
         * the managers location.
         */

        if (useCache) {
            /*
             * CHECK ONE: Look in cache of Local Table Managers.
             */
            final TableManager tm = localTableManagers.get(tableInfo);

            if (tm != null) { return tm; }

            /*
             * CHECK TWO: The Table Manager is not local. Look in the cache of Remote Table Manager References.
             */
            final ITableManagerRemote tableManager = cachedTableManagerReferences.get(tableInfo);

            if (tableManager != null) { return tableManager; }
        }

        if (!searchOnlyCache) {
            int lookupCount = 0;

            while (lookupCount < 2) {
                lookupCount++;

                /*
                 * CHECK THREE: The Table Manager proxy is not known. Contact the System Table for the managers location.
                 */
                try {
                    if (systemTableWrapper.getSystemTable() == null) {
                        systemTableWrapper = systemTableRecovery.get();
                    }
                    else {

                        final TableManagerWrapper tableManagerWrapper = systemTableWrapper.getSystemTable().lookup(tableInfo);

                        if (tableManagerWrapper == null) { return null; // During a create table operation it is expected that the lookup will return null here.
                        }

                        // Put this Table Manager in the local cache then return it.
                        final ITableManagerRemote tableManager = tableManagerWrapper.getTableManager();
                        cachedTableManagerReferences.put(tableInfo, tableManager);

                        return tableManager;
                    }
                }
                catch (final MovedException e) {
                    ErrorHandling.errorNoEvent("Current System Table reference points to System Table that has moved to: " + e.getMessage());
                    handleMovedException(e);
                }
                catch (final RPCException e) {
                    ErrorHandling.errorNoEvent("Error trying to connect to existing System Table reference.");

                    try {
                        systemTableWrapper = systemTableRecovery.get();
                    }
                    catch (final LocatorException e1) {
                        ErrorHandling.errorNoEvent("Couldn't find locator servers.");
                        throw new SQLException("Couldn't find locator servers.");
                    }
                    catch (final SystemTableAccessException e1) {
                        e.printStackTrace();
                        ErrorHandling.errorNoEvent("Failed to create System Table.");
                        throw new SQLException("Failed to create System Table.");
                    }
                }
                catch (final LocatorException e) {
                    throw new SQLException("Couldn't find locator servers.");
                }
                catch (final SystemTableAccessException e) {
                    e.printStackTrace();
                    throw new SQLException("Failed to create System Table.");
                }
            }

            throw new SQLException("Failed to find System Table.");
        }

        //only using local cache so we haven't looked at the System Table.
        return null;

    }

    @Override
    public boolean isThisSystemTableNode(final IChordRemoteReference otherNode) {

        if (systemTableNode == null) { return false; }
        return systemTableNode.equals(otherNode);
    }

    @Override
    public void addProxy(final TableInfo tableInfo, final ITableManagerRemote tableManager) {

        cachedTableManagerReferences.remove(tableInfo);
        cachedTableManagerReferences.put(tableInfo, tableManager);

        // This is only ever called on the local machine, so it is okay to add
        // the Table Manager to the set of local table managers here.
        localTableManagers.put(tableInfo.getGenericTableInfo(), (TableManager) tableManager);
    }

    @Override
    public void addNewTableManagerReference(final TableInfo ti, final ITableManagerRemote tm) {

        try {
            getSystemTable().changeTableManagerLocation(tm, ti);
        }
        catch (final RPCException e) {
            e.printStackTrace();
        }
        catch (final MovedException e) {
            e.printStackTrace();
        }
        addProxy(ti, tm);
    }

    @Override
    public boolean addTableInformation(final ITableManagerRemote iTableManagerRemote, final TableInfo ti, final Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException { // changed by

        localTableManagers.put(ti.getGenericTableInfo(), (TableManager) iTableManagerRemote);

        final boolean successful = systemTableWrapper.getSystemTable().addTableInformation(iTableManagerRemote, ti, replicaLocations);

        if (!successful) {
            localTableManagers.remove(ti.getGenericTableInfo());
        }
        return successful;
    }

    @Override
    public void removeTableInformation(final TableInfo tableInfo) throws RPCException, MovedException {

        localTableManagers.remove(tableInfo);
        cachedTableManagerReferences.remove(tableInfo);

        systemTableWrapper.getSystemTable().removeTableInformation(tableInfo);
    }

    @Override
    public void removeAllTableInformation() throws RPCException, MovedException {

        localTableManagers.clear();
        cachedTableManagerReferences.clear();

        systemTableWrapper.getSystemTable().removeAllTableInformation();
    }

    @Override
    public Map<TableInfo, TableManager> getLocalTableManagers() {

        return localTableManagers;
    }

    @Override
    public ISystemTableMigratable getLocalSystemTable() {

        return systemTableWrapper.getSystemTable();
    }

    @Override
    public void handleMovedException(final MovedException e) throws SQLException {

        try {
            systemTableWrapper = systemTableRecovery.find(e);
        }
        catch (final RPCException e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public ISystemTableMigratable failureRecovery() throws LocatorException, SystemTableAccessException {

        systemTableWrapper = systemTableRecovery.get();

        return systemTableWrapper.getSystemTable();
    }

    @Override
    public void suspectInstanceOfFailure(final DatabaseID predecessorURL) throws RPCException, MovedException {

        systemTableWrapper.getSystemTable().suspectInstanceOfFailure(predecessorURL);

    }

}
