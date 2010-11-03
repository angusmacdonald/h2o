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
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.table.ReplicaSet;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.interfaces.SystemTableRemote;
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

    private final Map<TableInfo, TableManagerRemote> cachedTableManagerReferences = new HashMap<TableInfo, TableManagerRemote>();

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

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#getSystemTable()
     */
    @Override
    public SystemTableRemote getSystemTable() {

        return getSystemTable(false);
    }

    @Override
    public SystemTableRemote getSystemTable(final boolean inShutdown) {

        try {

            if (systemTableWrapper.getSystemTable() == null) {
                systemTableWrapper.setSystemTable(findSystemTable());
            }

            systemTableWrapper.getSystemTable().checkConnection();
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
            ErrorHandling.errorNoEvent(db.getURL() + ": Failed to find System Table. Attempting to re-instantiate it on a valid instance (via locator servers).");

            try {
                systemTableWrapper = systemTableRecovery.get();
            }
            catch (final LocatorException e1) {
                ErrorHandling.errorNoEvent("Couldn't find any locator servers when looking for the System Table.");
            }
            catch (final SystemTableAccessException e1) {
                ErrorHandling.errorNoEvent("Failed to recreate the System Table anywhere.");
            }
        }

        if (systemTableWrapper.getSystemTable() != null) {
            try {
                systemTableNode = systemTableWrapper.getSystemTable().getChordReference();
            }
            catch (final RemoteException e) {
                ErrorHandling.errorNoEvent("Failed to obtain the System Table's chord reference.");
            }
        }

        return systemTableWrapper.getSystemTable();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableLocation()
     */
    @Override
    public DatabaseURL getSystemTableURL() {

        return systemTableWrapper.getURL();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#isSystemTableLocal()
     */
    @Override
    public boolean isSystemTableLocal() {

        return isLocal;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#findSystemTable()
     */
    @Override
    public SystemTableRemote findSystemTable() throws SQLException {

        try {
            systemTableWrapper = systemTableRecovery.get();
            return systemTableWrapper.getSystemTable();
        }
        catch (final LocatorException e) {
            e.printStackTrace();
            throw new SQLException("Couldn't find locator server(s).");
        }
        catch (final SystemTableAccessException e) {
            e.printStackTrace();
            throw new SQLException("Couldn't create System Table.");
        }

        // if (systemTableWrapper.getSystemTable() != null) {
        // return systemTableWrapper.getSystemTable();
        // }
        //
        // Registry registry = getSystemTableRegistry();
        //
        // try {
        // systemTableWrapper.setSystemTable((SystemTableRemote) registry.lookup(SCHEMA_MANAGER));
        // this.systemTableNode = systemTableWrapper.getSystemTable().getChordReference();
        // } catch (Exception e) {
        // throw new SQLException("Unable to find System Table. Attempted to find it through the registry at " +
        // systemTableWrapper.getURL());
        // }
        //
        // return systemTableWrapper.getSystemTable();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableRegistry()
     */
    @Override
    public Registry getSystemTableRegistry() {

        Registry remoteRegistry = null;

        try {
            remoteRegistry = LocateRegistry.getRegistry(systemTableWrapper.getURL().getHostname(), systemTableWrapper.getURL().getRMIPort());

        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }

        return remoteRegistry;

    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#setNewSystemTableLocation(org .h2.h2o.util.DatabaseURL)
     */
    @Override
    public void setSystemTableURL(final DatabaseURL newSMLocation) {

        if (newSMLocation.equals(db.getURL())) {
            isLocal = true;
        }

        systemTableWrapper.setURL(newSMLocation);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#setSystemTableLocation(uk.ac
     * .standrews.cs.stachordRMI.interfaces.IChordRemoteReference, org.h2.h2o.util.DatabaseURL)
     */
    @Override
    public void setSystemTableLocation(final IChordRemoteReference systemTableLocation, final DatabaseURL databaseURL) {

        systemTableNode = systemTableLocation;
        systemTableWrapper.setURL(databaseURL);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#setSystemTable(org.h2.h2o.manager .SystemTable)
     */
    @Override
    public void setSystemTable(final SystemTableRemote systemTable) {

        systemTableWrapper.setSystemTable(systemTable);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#isConnectedToSM()
     */
    @Override
    public boolean isConnectedToSM() {

        return systemTableWrapper.getSystemTable() != null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#setInKeyRange(boolean)
     */
    @Override
    public void setInKeyRange(final boolean inKeyRange) {

        this.inKeyRange = inKeyRange;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#isInKeyRange()
     */
    @Override
    public boolean isInKeyRange() {

        return inKeyRange;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance (boolean, boolean)
     */
    @Override
    public SystemTableRemote migrateSystemTableToLocalInstance(final boolean persistedSchemaTablesExist, final boolean recreateFromPersistedState) throws SystemTableAccessException {

        systemTableWrapper = systemTableRecovery.restart(persistedSchemaTablesExist, recreateFromPersistedState, systemTableWrapper.getSystemTable());

        db.getMetaDataReplicaManager().replicateMetaDataIfPossible(this, true); // replicate system table state.

        /*
         * Make the new System Table remotely accessible.
         */
        isLocal = systemTableWrapper.getURL().equals(db.getURL());

        try {
            final SystemTableRemote stub = (SystemTableRemote) UnicastRemoteObject.exportObject(systemTableWrapper.getSystemTable(), 0);

            getSystemTableRegistry().rebind(SCHEMA_MANAGER, stub);
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Binding System Table on port " + systemTableWrapper.getURL().getRMIPort());
        }
        catch (final Exception e) {
            ErrorHandling.errorNoEvent("Failed to export System Table object: " + e.getMessage());
        }

        // try {
        // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Checking System Table Accessibility...");
        // systemTableWrapper.getSystemTable().checkTableManagerAccessibility();
        // System.err.println("...");
        // } catch (RemoteException e) {
        // e.printStackTrace();
        // } catch (MovedException e) {
        // e.printStackTrace();
        // }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new System Table on " + db.getURL().getDbLocation() + ".");
        H2OEventBus.publish(new H2OEvent(db.getURL().getURL(), DatabaseStates.SYSTEM_TABLE_MIGRATION));
        return systemTableWrapper.getSystemTable();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance ()
     */
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

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#setLookupLocation(uk.ac.standrews .cs.stachordRMI.interfaces.IChordRemoteReference)
     */
    @Override
    public void setLookupLocation(final IChordRemoteReference proxy) {

        lookupLocation = proxy;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#getLookupLocation()
     */
    @Override
    public IChordRemoteReference getLookupLocation() {

        return lookupLocation;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#lookup(java.lang.String)
     */
    @Override
    public TableManagerRemote lookup(final String tableName, final boolean useCache) throws SQLException {

        return lookup(new TableInfo(tableName), useCache);
    }

    @Override
    public TableManagerRemote lookup(final TableInfo tableInfo, final boolean useCache) throws SQLException {

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
            final TableManagerRemote tableManager = cachedTableManagerReferences.get(tableInfo);

            if (tableManager != null) { return tableManager; }
        }

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
                    final TableManagerRemote tableManager = tableManagerWrapper.getTableManager();
                    cachedTableManagerReferences.put(tableInfo, tableManager);

                    return tableManager;
                }
            }
            catch (final MovedException e) {
                handleMovedException(e);
            }
            catch (final RemoteException e) {
                ErrorHandling.errorNoEvent("Error trying to recreate System Table. Trying again.");

                try {
                    systemTableWrapper = systemTableRecovery.get();
                }
                catch (final LocatorException e1) {
                    throw new SQLException("Couldn't find locator servers.");
                }
                catch (final SystemTableAccessException e1) {
                    throw new SQLException("Failed to create System Table.");
                }
            }
            catch (final LocatorException e) {
                throw new SQLException("Couldn't find locator servers.");
            }
            catch (final SystemTableAccessException e) {
                throw new SQLException("Failed to create System Table.");
            }
        }

        throw new SQLException("Failed to find System Table.");
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#isThisSystemTableNode(uk.ac. standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
     */
    @Override
    public boolean isThisSystemTableNode(final IChordRemoteReference otherNode) {

        if (systemTableNode == null) { return false; }
        return systemTableNode.equals(otherNode);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#addProxy(org.h2.h2o.comms.remote .TableManagerRemote)
     */
    @Override
    public void addProxy(final TableInfo tableInfo, final TableManagerRemote tableManager) {

        cachedTableManagerReferences.remove(tableInfo);
        cachedTableManagerReferences.put(tableInfo, tableManager);

        // This is only ever called on the local machine, so it is okay to add
        // the Table Manager to the set of local table managers here.
        localTableManagers.put(tableInfo.getGenericTableInfo(), (TableManager) tableManager);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.h2o.manager.ISystemTableReference#addNewTableManagerReference( org.h2.h2o.util.TableInfo,
     * org.h2.h2o.comms.remote.TableManagerRemote)
     */
    @Override
    public void addNewTableManagerReference(final TableInfo ti, final TableManagerRemote tm) {

        try {
            getSystemTable().changeTableManagerLocation(tm, ti);
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }
        catch (final MovedException e) {
            e.printStackTrace();
        }
        addProxy(ti, tm);
    }

    @Override
    public boolean addTableInformation(final TableManagerRemote tableManagerRemote, final TableInfo ti, final Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException, MovedException, SQLException { // changed by

        // al
        localTableManagers.put(ti.getGenericTableInfo(), (TableManager) tableManagerRemote);

        return systemTableWrapper.getSystemTable().addTableInformation(tableManagerRemote, ti, replicaLocations);
    }

    @Override
    public void removeTableInformation(final TableInfo tableInfo) throws RemoteException, MovedException {

        localTableManagers.remove(tableInfo);
        cachedTableManagerReferences.remove(tableInfo);

        systemTableWrapper.getSystemTable().removeTableInformation(tableInfo);
    }

    @Override
    public void removeAllTableInformation() throws RemoteException, MovedException {

        localTableManagers.clear();
        cachedTableManagerReferences.clear();

        systemTableWrapper.getSystemTable().removeAllTableInformation();
    }

    @Override
    public Map<TableInfo, TableManager> getLocalTableManagers() {

        return localTableManagers;
    }

    @Override
    public SystemTableRemote getLocalSystemTable() {

        return systemTableWrapper.getSystemTable();
    }

    @Override
    public void handleMovedException(final MovedException e) throws SQLException {

        try {
            systemTableWrapper = systemTableRecovery.find(e);
        }
        catch (final RemoteException e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public ISystemTable failureRecovery() throws LocatorException, SystemTableAccessException {

        systemTableWrapper = systemTableRecovery.get();

        return systemTableWrapper.getSystemTable();
    }

}
