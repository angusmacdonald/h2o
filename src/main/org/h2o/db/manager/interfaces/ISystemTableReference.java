/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.interfaces;

import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.numonic.interfaces.ICentralDataCollector;
import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.SystemTable;
import org.h2o.db.manager.TableManager;
import org.h2o.db.manager.recovery.LocatorException;
import org.h2o.db.manager.recovery.SystemTableAccessException;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISystemTableReference {

    /**
     * Get a reference to the System Table. If the current System Table location is not known this method will attempt to find it.
     * 
     * <p>
     * The System Table may be remote.
     * 
     * @return Reference to the system System Table.
     */
    public ISystemTableMigratable getSystemTable();

    /**
     * Called with a 'true' parameter when the system is being shut down to allow it to ignore any exceptions that may occur if the System
     * Table is unavailable. Just makes for tidier output when running tests.
     * 
     * @param inShutdown
     *            If the system is being shut down any remote exceptions when contacting the System Table will be ignored.
     * @return
     */
    public ISystemTableMigratable getSystemTable(boolean inShutdown);

    /**
     * Get the location of the System Table instance.
     * 
     * <p>
     * This is the stored System Table location (i.e. the system does not have to check whether the System Table still exists at this
     * location before returning a value).
     * 
     * @return Stored System Table location.
     */
    public DatabaseID getSystemTableURL();

    /**
     * True if the System Table process is running locally.
     */
    public boolean isSystemTableLocal();

    /**
     * Attempts to find the System Table by looking up its location in the RMI registry of the database instance which is responsible for
     * the key range containing 'System Table'.
     * 
     * @return Reference to the system System Table.
     * @throws SQLException
     *             If System Table registry access resulted in some kind of exception.
     */
    public ISystemTableMigratable findSystemTable() throws SQLException;

    /**
     * Change the System Table URL. This doesn't update the actual reference to the System Table, so should only be used if the database has
     * just entered or started a chord ring, or has just found a new System Table reference.
     * 
     * @param systemTableURL
     */
    public void setSystemTableURL(DatabaseID systemTableURL);

    /**
     * Provide a reference to the actual System Table. This is typically called when a database has just been started, or when a new System
     * Table has been created.
     */
    public void setSystemTable(ISystemTableMigratable systemTable);

    /**
     * Change the System Table URL and its location on chord. This doesn't update the actual reference to the System Table, so should only
     * be used if the database has just entered or started a chord ring, or has just found a new System Table reference.
     * 
     * @param newSMLocation
     */
    public void setSystemTableLocation(IChordRemoteReference systemTableLocation, DatabaseID databaseURL);

    /**
     * True if this instance has a reference to the System Table.
     */
    public boolean isConnectedToSM();

    /**
     * Specify whether the System Table lookup is in the keyrange of the given chord node.
     */
    public void setInKeyRange(boolean inKeyRange);

    /**
     * True if the System Table chord lookup resolves to the local node.
     */
    public boolean isInKeyRange();

    /**
     * Create another System Table at the current location, replacing the old manager.
     * 
     * @param persistedSchemaTablesExist
     *            Whether replicated copies of the System Tables state exist locally.
     * @param recreateFromPersistedState
     *            If true the new System Table will be re-instantiated from persisted state on disk. Otherwise it will be migrated from an
     *            active in-memory copy. If the old System Table has failed the new manager must be recreated from persisted state.
    @param noReplicateToPreviousInstance If true, then the new system table will start and be told not to replicate data to the previous system table's location. Only applies to migrations on active System Tables (those not using persisted state).
     * @return
     * @throws SystemTableAccessException
     */
    public ISystemTableMigratable migrateSystemTableToLocalInstance(boolean persistedSchemaTablesExist, boolean recreateFromPersistedState, boolean noReplicateToPreviousInstance) throws SystemTableAccessException;

    /**
     * If called the System Table will be moved to the local database instance.
     * @param noReplicateToPreviousInstance 
     * 
     * @throws SystemTableAccessException
     */
    public void migrateSystemTableToLocalInstance(boolean noReplicateToPreviousInstance) throws SystemTableAccessException;

    /**
     * An exception has been thrown trying to access the System Table because it has been moved to a new location. This method handles this
     * by updating the reference to that of the new System Table.
     * 
     * @throws SQLException
     */
    public void handleMovedException(MovedException e) throws SQLException;

    /**
     * Update the reference to the new chord node responsible for the System Table key lookup.
     * 
     * @param proxy
     *            Chord node responsible for the pointer to the System Table, but not necessarily the System Table itself.
     */
    public void setLookupLocation(IChordRemoteReference proxy);

    /**
     * Get the location of the chord node responsible for maintaining the pointer to the actual System Table. This may be used when a
     * database is joining the system and has to find the System Table.
     */
    public IChordRemoteReference getLookupLocation();

    /**
     * Find a Table Manager for the given table in the database system.
     * 
     * <p>
     * This method is a wrapper for a possibly remote System Table call. If the System Table call fails this method will check if the System
     * Table has moved and redirect the call if it has.
     * 
     * @param fqTableName
     *            the table whose manager is to be found (fully qualified name includes schema name).
     * @param useCache 
     *             Whether to look in the local cache before querying the System Table.
     * @return Remote reference to the Table Manager in question.
     * @throws SQLException
     *             Thrown if the System Table could not be found anywhere, and lookup failed twice.
     */
    public ITableManagerRemote lookup(String fqTableName, boolean useCache) throws SQLException;

    /**
     * Find the Table Manager for the given table in the database system.
     * 
     * <p>
     * This method is a wrapper for a possibly remote System Table call. If the System Table call fails this method will check if the System
     * Table has moved and redirect the call if it has.
     * 
     * @param tableInfo
     *            The table name and schema name are used in the lookup.
     * @param useCache 
     *             Whether to look in the local cache before querying the System Table.
     * @return
     * @throws SQLException
     *             Thrown if the System Table cannot be found.
     */
    public ITableManagerRemote lookup(TableInfo tableInfo, boolean useCache) throws SQLException;

    /**
     * Find the Table Manager for the given table in the database system.
     * 
     * <p>
     * This method is a wrapper for a possibly remote System Table call. If the System Table call fails this method will check if the System
     * Table has moved and redirect the call if it has.
     * 
     * @param tableInfo
     *            The table name and schema name are used in the lookup.
     * @param useCache 
     *             Whether to look in the local cache before querying the System Table.
     * @param searchOnlyCache Whether to only look in the cache and not in the System Table. If this if true and useCache is false the method will do nothing.
     * @return
     * @throws SQLException
     *             Thrown if the System Table cannot be found.
     */
    public ITableManagerRemote lookup(TableInfo tableInfo, boolean useCache, boolean searchOnlyCache) throws SQLException;

    /**
     * Check if the node given as a parameter is the node on which the System Table is held.
     * 
     * @param otherNode
     *            Node to check against.
     * @return True if the System Table is held on this node.
     */
    public boolean isThisSystemTableNode(IChordRemoteReference otherNode);

    /**
     * Add a new TableManager proxy to the local cache.
     * 
     * @param tableInfo
     *            The fully qualified name of the table to be added.
     * @param tableManager
     *            The Table Manager to be added.
     */
    void addProxy(TableInfo tableInfo, ITableManagerRemote tableManager);

    /**
     * Add a new Table Manager reference to the System Table.
     * 
     * @param ti
     *            The name of the table being added.
     * @param tm
     *            The reference to the extant Table Manager.
     */
    public void addNewTableManagerReference(TableInfo ti, ITableManagerRemote tm);

    /**
     * Adds a new Table Manager to the System Table. Before doing this it stores a local reference to the Table Manager to bypass RMI calls
     * (which are extremely inefficient).
     * 
     * @param iTableManagerRemote
     *            The table manager being added to the System Table.
     * @param ti
     *            Name of the table being added.
     * @param replicaLocations
     * @return True if the table was successfully added.
     * @throws RPCException
     *             Thrown if the System Table could not be contacted.
     * @throws MovedException
     *             Thrown if the System Table has moved and a new reference is needed.
     */
    public boolean addTableInformation(ITableManagerRemote iTableManagerRemote, TableInfo ti, Set<DatabaseInstanceWrapper> replicaLocations) throws RPCException, MovedException, SQLException;

    public void removeTableInformation(TableInfo tableInfo) throws RPCException, MovedException;

    public void removeAllTableInformation() throws RPCException, MovedException;

    public Map<TableInfo, TableManager> getLocalTableManagers();

    /**
     * Get the reference to the System Table that is held locally, regardless of whether the reference is still active.
     * 
     * @return
     */
    public ISystemTableMigratable getLocalSystemTable();

    /**
     * Attempt to find the system table, and recreate it if it is not active.
     * @return
     * @throws LocatorException
     * @throws SQLException
     * @throws SystemTableAccessException
     */
    public ISystemTableMigratable failureRecovery() throws LocatorException, SQLException, SystemTableAccessException;

    /**
     * Inform the System table that a database instance has possibly failed.
     * 
     * <p>The system table will try to contact the instance itself, and if it can't, it removes
     * it from the membership set.
     * @param predecessorURL the instance that is suspected of failure.
     */
    public void suspectInstanceOfFailure(DatabaseID predecessorURL) throws RPCException, MovedException;

    /**
     * Calls the {@link ICentralDataCollector#getRankedListOfInstances(IMetric, Requirements)} method on the {@link SystemTable} class,
     * but may also cache some results locally for a period of time.
     * @param metric
     * @param requirements
     * @return
     * @throws MovedException 
     * @throws RPCException 
     */
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances(IMetric metric, Requirements requirements) throws RPCException, MovedException;

}
