/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.query.asynchronous.AsynchronousQueryExecutor;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.asynchronous.Transaction;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Manages query proxies where multiple instances are required in a single transaction.
 * 
 * <p>
 * Situations where this is important, include: where multiple tables are on the same machines, and where a table is accessed by multiple
 * queries (meaning locks only need to be taken out once).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryProxyManager {

    private String transactionName;

    private DatabaseInstanceWrapper localDatabase;

    private Parser parser;

    private Map<DatabaseInstanceWrapper, Integer> allReplicas;

    private DatabaseInstanceWrapper requestingDatabase;

    private Map<String, QueryProxy> queryProxies;

    private Command prepareCommand = null;

    /**
     * Hack to ensure single table updates work when auto-commit is off.
     */
    private TableInfo tableName = null;

    /**
     * The update ID for this transaction. This is the highest update ID returned by the query proxies held in this manager.
     * 
     * The update ID is only used for single update local transactions - where more than one update is involved there is a different update
     * ID per table being updated.
     */
    private int updateID = 0;

    private List<String> queries;

    private boolean hasCommitted = false;

    private Session session;

    /**
     * 
     * @param db
     * @param session
     *            The session on which this set of queries is being executed. This must be the same session as the one used to execute the
     *            set of queries, otherwise commit won't work correctly (it won't be able to unlock anything).
     */
    public QueryProxyManager(Database db, Session session) {

        this(db, session, false);

        queries = new LinkedList<String>();

    }

    /**
     * 
     * @param db
     * @param systemSession
     * @param metaRecordProxy
     *            True if this proxy manager is being used to execute meta-records on system startup. This just adds the local database as
     *            the only replica.
     */
    public QueryProxyManager(Database db, Session session, boolean metaRecordProxy) {

        this.session = session;

        this.localDatabase = db.getLocalDatabaseInstanceInWrapper();

        this.transactionName = db.getTransactionNameGenerator().generateName();

        this.parser = new Parser(session, true);

        this.allReplicas = new HashMap<DatabaseInstanceWrapper, Integer>();

        if (metaRecordProxy) {
            this.allReplicas.put(localDatabase, 0);
        }

        this.requestingDatabase = db.getLocalDatabaseInstanceInWrapper();

        this.queryProxies = new HashMap<String, QueryProxy>();

    }

    /**
     * Adds a proxy object for one of the SQL statements being executed in this transaction. The proxy object contains details of any locks
     * this database instance now holds for a given table.
     * 
     * @param proxy
     *            Proxy for a particular table.
     */
    public void addProxy(QueryProxy proxy) throws SQLException {

        if (hasLock(proxy)) {
            // throw new SQLException("Table already locked. Cannot perform query.");

            tableName = proxy.getTableName();
            if (proxy.getUpdateID() > this.updateID) { // the update ID should be
                // the highest of all the
                // proxy update IDs
                this.updateID = proxy.getUpdateID();
            }

            if (proxy.getReplicaLocations() != null && proxy.getReplicaLocations().size() > 0) {
                allReplicas.putAll(proxy.getReplicaLocations());
            }
            else {
                /*
                 * Adds the local database to the set of databases holding something relevent to the query, IF the set is currently empty.
                 * Executed if no replica location was specified by the query proxy, which will happen on queries which don't involve a
                 * particular table (these are always local anyway).
                 */
                allReplicas.put(parser.getSession().getDatabase().getLocalDatabaseInstanceInWrapper(), this.updateID);
            }

        }
        else {
            ErrorHandling.errorNoEvent("No lock could be found for this table: " + proxy);
        }
        queryProxies.put(proxy.getTableName().getFullTableName(), proxy);
    }

    /**
     * Tests whether any locks are already held for the given table, either by the new proxy, or by the manager itself.
     * 
     * @param proxy
     *            New proxy.
     * @return true if locks are already held by one of the proxies; otherwise false.
     */
    public boolean hasLock(QueryProxy newProxy) {

        if (newProxy.getLockGranted() != LockType.NONE) return true; // this proxy already holds the required lock

        // The proxy doesn't hold the lock - does the manager already have it?
        if (getTableManagers().contains(newProxy.getTableManager())) {

            QueryProxy qpWithTM = getQueryProxyWithTableManager(newProxy.getTableManager());

            newProxy.setLockType(qpWithTM.getLockGranted());
            return true;
        }
        else {
            return false;
        }
    }

    public void releaseReadLocks() {

        Set<QueryProxy> toRemove = new HashSet<QueryProxy>();

        for (QueryProxy qp : queryProxies.values()) {
            if (qp.getLockGranted().equals(LockType.READ)) {
                try {
                    toRemove.add(qp);
                    qp.getTableManager().releaseLockAndUpdateReplicaState(false, requestingDatabase, null, false);
                }
                catch (Exception e) {
                    ErrorHandling.errorNoEvent("Table Manager could not be contacted: " + e.getMessage());
                    // If it couldn't be found at this location it doesn't matter, because a failed table manager doesn't hold any locks.
                }
            }
        }

        for (QueryProxy qpToRemove : toRemove) {

            queryProxies.remove(qpToRemove.getTableName().getFullTableName());
        }
    }

    /**
     * Commit or rollback the transaction being run through this proxy manager. Involves contacting each machine taking part in the
     * transaction and sending a commit for the correct transaction name.
     * 
     * @param commit
     *            True if the transaction is to be committed. False if the transaction should be rolled back.
     * @param h2oCommit
     *            If the application has turned off auto-commit, this parameter will be false and the only commits the database will receive
     *            will be from the application - in this case no transaction name is attached to the commit because this only happens when
     *            h2o is auto-committing.
     * @throws SQLException
     */
    public void finishTransaction(boolean commit, boolean h2oCommit, Database db) throws SQLException {

        try {
            if (getTableManagers().size() == 0 && allReplicas.size() > 0 && h2oCommit) {
                // H2O commit required to prevent stack overflow on recursive commit calls. testExecuteCall test fails without this.
                /*
                 * tableManagers.size() == 0 - indicates this is a local internal database operation (e.g. the TCP server doing something).
                 * allReplicas.size() > 0 - confirms it is an internal operation. otherwise it may be a COMMIT from the application or a
                 * prepared statement.
                 */

                commitLocal(commit, h2oCommit);

                return;
            }
            else if (getTableManagers().size() == 0 && allReplicas.size() == 0) {

                return;
            }
            else if (db.getAsynchronousQueryManager() == null) {

            return; // management db etc may call this.
            }

            Transaction committingTransaction = db.getAsynchronousQueryManager().getTransaction(transactionName);

            Set<CommitResult> commitedQueries = null;

            if (committingTransaction == null) {
                commitedQueries = new HashSet<CommitResult>();

                for (Entry<DatabaseInstanceWrapper, Integer> replica : allReplicas.entrySet()) {
                    commitedQueries.add(new CommitResult(commit, replica.getKey(), replica.getValue(), updateID, tableName));
                }
            }
            else {
                /*
                 * The set of replicas that were updated. This is returned to the DM when locks are released.
                 */
                commitedQueries = committingTransaction.getCompletedQueries();
                committingTransaction.setHasCommitted(h2oCommit);
            }

            releaseLocksAndUpdateReplicaState(commitedQueries, commit);

            boolean commitActionSuccessful = sendCommitMessagesToReplicas(commit, h2oCommit, db, commitedQueries);

            if (!commitActionSuccessful) {
                ErrorHandling.errorNoEvent("Commit message to replicas was unsuccessful for transaction '" + transactionName + "'.");
            }

            // if (commitActionSuccessful && commit)
            // updatedReplicas = allReplicas; // For asynchronous updates this should check for each replicas success.
            //
            // endTransaction(commitedQueries);

            printTraceOutputOfExecutedQueries();

            if (h2oCommit) {
                queryProxies.clear();
            }

        }
        finally {
            hasCommitted = true;

            this.session.completeTransaction();
        }
    }

    private void printTraceOutputOfExecutedQueries() {

        if (Diagnostic.getLevel() == DiagnosticLevel.FULL) {
            if (this.localDatabase.getURL().getRMIPort() > 0) {
                System.out.println("\tQueries in transaction (on DB at " + this.localDatabase.getURL().getRMIPort() + ": '" + transactionName + "'):");
                if (queries != null) {
                    for (String query : queries) {
                        if (query.equals("")) continue;
                        System.out.println("\t\t" + query);
                    }
                }
            }
        }
    }

    /**
     * 
     * @param commit
     * @param h2oCommit
     * @param db
     * @param commitedQueries
     *            Locations where replicas have committed. Send the commit message here.
     * @return
     */
    private boolean sendCommitMessagesToReplicas(boolean commit, boolean h2oCommit, Database db, Set<CommitResult> commitedQueries) {

        if (!h2oCommit) return true; // the application has set auto-commit to true.

        String sql = (commit ? "commit" : "rollback") + ((h2oCommit) ? " TRANSACTION " + transactionName : ";");

        AsynchronousQueryExecutor queryExecutor = new AsynchronousQueryExecutor(db);

        Map<DatabaseInstanceWrapper, Integer> commitLocations = getCommittedLocations(commitedQueries);

        boolean actionSuccessful = queryExecutor.executeQuery(sql, transactionName, commitLocations, null, this.parser.getSession(), true);
        return actionSuccessful;
    }

    private Map<DatabaseInstanceWrapper, Integer> getCommittedLocations(Collection<CommitResult> commitedQueries) {

        Map<DatabaseInstanceWrapper, Integer> commitLocations = new HashMap<DatabaseInstanceWrapper, Integer>();
        for (CommitResult commitResult : commitedQueries) {
            if (commitResult.isCommit()) {
                commitLocations.put(commitResult.getDatabaseInstanceWrapper(), commitResult.getUpdateID());
            }
        }
        return commitLocations;
    }

    /**
     * Commit the transaction on the local machine.
     * 
     * @param commit
     *            Whether to commit or rollback (true if commit)
     * @param h2oCommit
     * @return true if the commit was successful. False if it wasn't, or if it was a rollback.
     * @throws SQLException
     */
    private boolean commitLocal(boolean commit, boolean h2oCommit) throws SQLException {

        prepare();

        Command commitCommand = parser.prepareCommand((commit ? "COMMIT" : "ROLLBACK") + ((h2oCommit) ? " TRANSACTION " + transactionName : ";"));
        int result = commitCommand.executeUpdate();

        return (result == 0);
    }

    /**
     * Name of the transaction assigned at the start.
     * 
     * @return
     */
    public String getTransactionName() {

        return transactionName;
    }

    /**
     * Release locks for every table that is part of this update. This also updates the information on which replicas were updated (which
     * are currently active), hence the parameter
     * 
     * @param commit
     *            True if the transaction was committed, false if it is a rollback.
     * @param committedQueries
     *            The set of updates that have been attempted on tables involved in this transaction. This will be null if a query was
     *            performed (because there were no updates).
     * 
     */
    public void releaseLocksAndUpdateReplicaState(Set<CommitResult> committedQueries, boolean commit) {

        try {
            for (TableManagerRemote tableManagerProxy : getTableManagers()) {
                tableManagerProxy.releaseLockAndUpdateReplicaState(commit, requestingDatabase, committedQueries, false);
            }
        }
        catch (RemoteException e) {
            ErrorHandling.exceptionError(e, "Failed to release lock - couldn't contact the Table Manager");
        }
        catch (MovedException e) {
            ErrorHandling.exceptionError(e, "This should never happen - migrating process should hold the lock.");
        }
        finally {
            hasCommitted = true;
        }
    }

    /**
     * @param table
     * @return
     */
    public QueryProxy getQueryProxy(String tableName) {

        return queryProxies.get(tableName);
    }

    /**
     * Prepare a transaction to be committed. This is only called locally.
     * 
     * @throws SQLException
     */
    public void prepare() throws SQLException {

        if (prepareCommand == null) {
            prepareCommand = parser.prepareCommand("PREPARE COMMIT " + transactionName);
        }

        prepareCommand.executeUpdate();
    }

    /**
     * 
     */
    public void begin() throws SQLException {

        Command command = parser.prepareCommand("BEGIN");
        command.executeUpdate();

    }

    /**
     * Adds the current SQL query to the set of all queries that are part of this transaction. This is only used when full diagnostics are
     * on.
     * 
     * @param sql
     */
    public void addSQL(String sql) {

        this.queries.add(sql);
    }

    public List<String> getSQL() {

        return this.queries;
    }

    /**
     * @return
     */
    public boolean hasAllLocks() {

        for (QueryProxy qp : queryProxies.values()) {
            if (qp.getLockGranted().equals(LockType.NONE)) { return false; }
        }

        return true;
    }

    @Override
    public String toString() {

        return "QueryProxyManager [transactionName=" + transactionName + ", localDatabase=" + localDatabase + "]";
    }

    /**
     * Get all of the table managers referenced in this object.
     * 
     * @return
     */
    private Set<TableManagerRemote> getTableManagers() {

        Set<TableManagerRemote> tableManagers = new HashSet<TableManagerRemote>();

        for (QueryProxy qp : queryProxies.values()) {

            if (qp.getTableManager() != null) {
                tableManagers.add(qp.getTableManager());
            }
        }

        return tableManagers;
    }

    /**
     * Get the query proxy that holds a lock for the specified table manager.
     */
    private QueryProxy getQueryProxyWithTableManager(TableManagerRemote tableManager) {

        for (QueryProxy qp : queryProxies.values()) {

            if (qp.getTableManager() != null && qp.getTableManager().equals(tableManager)) { return qp; }
        }

        return null;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ((transactionName == null) ? 0 : transactionName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        QueryProxyManager other = (QueryProxyManager) obj;
        if (transactionName == null) {
            if (other.transactionName != null) return false;
        }
        else if (!transactionName.equals(other.transactionName)) return false;
        return true;
    }

    public boolean hasCommitted() {

        return hasCommitted;
    }

    public void setCommitted(boolean hasCommitted) {

        this.hasCommitted = hasCommitted;
    }

}
