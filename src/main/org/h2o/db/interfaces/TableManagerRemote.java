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

package org.h2o.db.interfaces;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;

import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.manager.util.Migratable;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.replication.ReplicaManager;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;

/**
 * Remote interface for Table Manager instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface TableManagerRemote extends H2ORemote, Migratable {

    public TableProxy getTableProxy(LockType lockType, LockRequest lockRequest) throws RemoteException, SQLException, MovedException;

    public boolean addTableInformation(DatabaseURL tableManagerURL, TableInfo tableDetails) throws RemoteException, MovedException, SQLException;

    public void addReplicaInformation(TableInfo tableDetails) throws RemoteException, MovedException, SQLException;

    public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException, SQLException;

    public boolean removeTableInformation() throws RemoteException, SQLException, MovedException;

    /**
     * Gets the location of a single replica for the given table. This is used in creating linked tables, so the return type is string rather
     * than DatabaseInstanceRemote.
     * 
     * @return Database connection URL for a given remote database.
     * @throws RemoteException
     */
    public DatabaseURL getLocation() throws RemoteException, MovedException;

    /**
     * Releases a lock held by the database instance specified in the parameter. Called at the end of TableProxy.executeQuery() to indicate
     * that the transaction has finished (it may have succeeded or failed).
     * 
     * @param commit
     * 
     * @param requestingDatabase
     *            Database which made the original request. Lock was taken out in its name.
     * @param committedQueries
     *            The set of replicas that were successfully updated by this query.
     * @param asynchronousCommit
     *            True if this is a commit of a replica where other replicas have already committed, and this is being done asynchronously.
     * @throws MovedException
     * @throws SQLException
     *          Thrown if the table manager is persisting a CREATE TABLE statement and it couldn't connect to the System Table.
     */
    public void releaseLockAndUpdateReplicaState(boolean commit, LockRequest requestingDatabase, Collection<CommitResult> committedQueries, boolean asynchronousCommit) throws RemoteException, MovedException, SQLException;

    /**
     * Deconstructs this Table Manager. This is required for testing where a remote reference to a Table Manager may not completely die when
     * expected - this method should essentially render the Table Manager unusable.
     * 
     * <p>
     * Also called when a table is dropped. If dropCommand is true all persisted state is removed as well.
     */
    public void remove(boolean dropCommand) throws RemoteException;

    /**
     * The name of the schema which this table is in.
     */
    public String getSchemaName() throws RemoteException;

    /**
     * The name of the table this Table Manager is responsible for (not including schema name).
     */
    public String getTableName() throws RemoteException;

    /**
     * The object responsible for managing the set of replicas this Table Manager maintains.
     * 
     * <p>
     * This is called when the Table Manager is being migrated elsewhere, but shouldn't need to be called anywhere else.
     * 
     * @throws MovedException
     */
    public ReplicaManager getReplicaManager() throws RemoteException, MovedException;

    /**
     * Get the table set that this table is part of.
     */
    public int getTableSet() throws RemoteException;

    /**
     * Build up the state of this Table Manager from the state of another extant manager. Used when migrating the state of the old manager
     * to this manager.
     * 
     * @param oldTableManager
     *            Extant Table Manager.
     * @throws MovedException
     *             Thrown if this Table Manager has already been moved to somewhere else.
     */
    public void buildTableManagerState(TableManagerRemote oldTableManager) throws RemoteException, MovedException;

    /**
     * The URL of the database on which this Table Manager is located.
     */
    public DatabaseURL getDatabaseURL() throws RemoteException;

    /**
     * Re-populate this Table Managers replica manager with state held locally on disk.
     * 
     * @throws SQLException
     */
    public void recreateReplicaManagerState(String oldPrimaryDatabaseName) throws RemoteException, SQLException;

    /**
     * Number of replicas of this table.
     * 
     * @return
     */
    public int getNumberofReplicas() throws RemoteException;

    /**
     * Persist the information on this table manager to complete the creation of the table.
     * 
     * @param ti
     *            Used to get the table set number for this table manager.
     */
    public void persistToCompleteStartup(TableInfo ti) throws RemoteException, StartupException;
}
