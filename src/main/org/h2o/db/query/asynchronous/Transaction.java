/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.FutureTask;

import org.h2.engine.Database;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class Transaction {

    private final String transactionID;

    /**
     * The set of all queries being executed as part of this transaction.
     * 
     * This is a set (rather than a list) because there may be many commit messages for the same table and there is no need to duplicate
     * them.
     */
    private Set<CommitResult> completedQueries;

    /**
     * The set of queries still being executed as part of this transaction.
     */
    private List<FutureTask<QueryResult>> incompleteQueries;

    /**
     * Whether the transaction this query is part of has committed.
     */
    private boolean transactionHasCommitted = false;

    private final int expectedUpdateID;

    /**
     * @param transactionID
     * @param executingQueries
     * @param recentlyCompletedQueries
     * @param expectedUpdateID
     * @param tableName
     */
    public Transaction(final String transactionID, final List<FutureTask<QueryResult>> executingQueries, final List<CommitResult> recentlyCompletedQueries, final int expectedUpdateID) {

        this.transactionID = transactionID;
        incompleteQueries = executingQueries;
        this.expectedUpdateID = expectedUpdateID;
        completedQueries = new HashSet<CommitResult>(recentlyCompletedQueries);

        if (completedQueries == null) {
            completedQueries = new HashSet<CommitResult>();
        }
    }

    /**
     * Check whether any active queries have finished executing.
     * 
     * @param db
     * @return true if the transaction has fully committed and can be removed.
     */
    public boolean checkForCompletion(final Database db) {

        final List<FutureTask<QueryResult>> recentlyCompletedQueries = new LinkedList<FutureTask<QueryResult>>();

        if (incompleteQueries == null && transactionHasCommitted) { return true; }

        /*
         * Wait until all remote queries have been completed.
         */
        while (incompleteQueries != null && incompleteQueries.size() > 0) {
            for (int y = 0; y < incompleteQueries.size();) {
                final FutureTask<QueryResult> incompleteQuery = incompleteQueries.get(y);

                if (incompleteQuery.isDone()) {

                    incompleteQueries.remove(y);

                    recentlyCompletedQueries.add(incompleteQuery);

                    /*
                     * When a query is done it should be added to a local list of recently completed queries, and information should be sent
                     * to the table manager *if* the transaction has completed already.
                     */
                }
                else {
                    y++;
                }
            }
        }

        final List<CommitResult> recentlyCompletedCommits = new LinkedList<CommitResult>();

        for (final FutureTask<QueryResult> completedQuery : recentlyCompletedQueries) {

            QueryResult asyncResult = null;
            try {
                asyncResult = completedQuery.get();
            }
            catch (final Exception e) {
                e.printStackTrace();
                break;
            }

            if (asyncResult.getException() == null) { // If the query executed successfully.
                final int result = asyncResult.getResult();
                final DatabaseInstanceWrapper wrapper = asyncResult.getWrapper();
                if (result != 0) {
                    // Prepare operation failed at remote machine
                    final CommitResult commitResult = new CommitResult(false, wrapper, asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
                    recentlyCompletedCommits.add(commitResult);

                }
                else {
                    final CommitResult commitResult = new CommitResult(true, wrapper, asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
                    recentlyCompletedCommits.add(commitResult);
                }

            }
            else {
                final CommitResult commitResult = new CommitResult(true, asyncResult.getWrapper(), asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
                recentlyCompletedCommits.add(commitResult);
            }
        }

        if (transactionHasCommitted) { // Send all new commits to the table manager.
            // Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Asynchronous updates completed for transaction '" + transactionID + "'.");
            commit(recentlyCompletedCommits, db);

            return incompleteQueries == null || incompleteQueries.size() == 0;
        }

        completedQueries.addAll(recentlyCompletedCommits);
        return false;
    }

    /**
     * Called from within this class when some updates have recently completed, but the transaction has already been committed. These
     * updates should now be reflected in the Table Manager for the given table.
     * 
     * @param completedUpdates
     */
    public synchronized void commit(final Collection<CommitResult> newlyCompletedUpdates, final Database db) {

        if (!db.isRunning()) { return; }

        for (final CommitResult completedQuery : newlyCompletedUpdates) {

            if (completedQuery.isCommitQuery()) {
                if (!completedQuery.isCommit()) {
                    ErrorHandling.errorNoEvent("Error sending COMMIT message for transaction " + transactionID);
                }
            }
            else {
                final TableInfo tableName = completedQuery.getTable();

                ITableManagerRemote tableManager = null;

                try {
                    tableManager = db.getSystemTableReference().lookup(tableName, true);
                }
                catch (final SQLException e) {
                    e.printStackTrace();
                }

                if (tableManager != null) {
                    final DatabaseInstanceWrapper databaseHoldingLock = db.getLocalDatabaseInstanceInWrapper(); //not used in this case because the updates have already completed.
                    final LockRequest lockRequest = new LockRequest(databaseHoldingLock, -1); //session ID doesn't matter because no locks are held at this point.
                    try {
                        tableManager.releaseLockAndUpdateReplicaState(true, lockRequest, newlyCompletedUpdates, true);
                    }
                    catch (final RPCException e) {
                        e.printStackTrace();
                    }
                    catch (final MovedException e) {
                        try {
                            tableManager = db.getSystemTableReference().lookup(tableName, false);
                            tableManager.releaseLockAndUpdateReplicaState(true, lockRequest, newlyCompletedUpdates, true);
                        }
                        catch (final Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                    catch (final SQLException e) {
                        //Doesn't matter because the transaction has already committed at this point.
                        e.printStackTrace();
                    }
                }
                else {
                    // ErrorHandling.errorNoEvent("Table Manager not found for table : " + tableName);
                }
            }
        }
    }

    /**
     * Called by the TableProxyManager for a transaction when it is committing the transaction. It must send details of the completed
     * updates to the Table Manager.
     */
    public synchronized Set<CommitResult> getCompletedQueries() {

        return completedQueries;

        /*
         * TODO Called by the query proxy manager to commit a transaction. All the updated replicas (based on information in this class)
         * will be committed to the table manager. This method should return some data structure containing the names of updated replicas
         * and information on their update ID, etc...
         */
    }

    public String getTransactionID() {

        return transactionID;
    }

    public boolean hasCommitted() {

        return transactionHasCommitted;
    }

    public void setHasCommitted(final boolean transactionHasCommitted) {

        this.transactionHasCommitted = transactionHasCommitted;
    }

    public void addQueries(final List<FutureTask<QueryResult>> newIncompleteQueries) {

        if (newIncompleteQueries == null) { return; }
        if (incompleteQueries == null) {
            incompleteQueries = new LinkedList<FutureTask<QueryResult>>();
        }

        incompleteQueries.addAll(newIncompleteQueries);
    }

    public void addCompletedQueries(final List<CommitResult> recentlyCompletedQueries) {

        completedQueries.addAll(recentlyCompletedQueries);
    }

}
