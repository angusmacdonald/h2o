/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2o.db.DefaultSettings;
import org.h2o.db.id.TableInfo;
import org.h2o.db.query.TableProxy;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class AsynchronousQueryExecutor {

    private final Database database;

    int updatesNeededBeforeCommit = 0;

    private static Integer sleepTimeWhileWaitingForQueriesToFinish = Integer.valueOf(DefaultSettings.getString("AsynchronousQueryExecutor.SLEEP_TIME_WAITING_FOR_QUERIES_TO_COMPLETE")); //$NON-NLS-1$

    /**
     * @param database
     */
    public AsynchronousQueryExecutor(final Database database) {

        this.database = database;

        if (database.getDatabaseSettings().get("ASYNCHRONOUS_REPLICATION_ENABLED").equals("true")) { //$NON-NLS-1$ //$NON-NLS-2$
            updatesNeededBeforeCommit = Integer.parseInt(database.getDatabaseSettings().get("ASYNCHRONOUS_REPLICATION_FACTOR")); //$NON-NLS-1$
        }

    }

    private static ExecutorService queryExecutor = Executors.newCachedThreadPool(new QueryThreadFactory() {

        @Override
        public Thread newThread(final Runnable r) {

            return new Thread(r);
        }
    });

    /**
     * Asynchronously executes the query on each database instance that requires the update.
     * 
     * @param query
     *            Query to be executed.
     * @param transactionNameForQuery
     *            The name of the transaction in which this query is in.
     * @param tableName
     *            Name of the table involved in the update. NULL if this is a COMMIT/ROLLBACK.
     * @param session
     *            The session we are in - used to create the query parser.
     * @param commit
     *            The array that will be used to store (for each replica) whether the transaction was executed successfully.
     * @param commitOperation
     *            True if this is a COMMIT, false if it is another type of query. If it is false a PREPARE command will be executed to get
     *            ready for the eventual commit.
     * @return True if everything was executed successfully (a global commit).
     */
    public boolean executeQuery(final String query, final String transactionNameForQuery, final Map<DatabaseInstanceWrapper, Integer> allReplicas, final TableInfo tableName, final Session session, final boolean commitOperation) {

        final Parser parser = new Parser(session, true);

        final List<FutureTask<QueryResult>> executingQueries = new LinkedList<FutureTask<QueryResult>>();

        final int expectedUpdateID = getExpectedUpdateID(allReplicas);

        if (updatesNeededBeforeCommit == 0) { // will be zero if asynchronous updates are off.
            updatesNeededBeforeCommit = allReplicas.size();
        }

        int i = 0;
        for (final Entry<DatabaseInstanceWrapper, Integer> replicaToExecuteQueryOn : allReplicas.entrySet()) {

            final String localURL = session.getDatabase().getURL().getURL();

            // Decide whether the query is to be executed local or remotely.
            final boolean isReplicaLocal = replicaToExecuteQueryOn == null || localURL.equals(replicaToExecuteQueryOn.getKey().getURL().getURL());

            // Start execution of queries.
            if (replicaToExecuteQueryOn != null) {
                final Integer replicaUpdateID = replicaToExecuteQueryOn.getValue();

                executeQueryOnSpecifiedReplica(query, transactionNameForQuery, replicaToExecuteQueryOn.getKey(), replicaUpdateID, isReplicaLocal, parser, executingQueries, commitOperation, tableName);
                i++;
            }
        }

        // Wait for enough queries to execute, then return the result.
        return waitUntilRemoteQueriesFinish(executingQueries, updatesNeededBeforeCommit, transactionNameForQuery, expectedUpdateID, tableName);
    }

    /**
     * Get the update ID that must be reached on the Table Manager for the update to commit. If it is not reached there has been an
     * out-of-order query execution and the replica must be removed.
     * 
     * @param allReplicas
     * @return
     */
    private int getExpectedUpdateID(final Map<DatabaseInstanceWrapper, Integer> allReplicas) {

        int expectedUpdateID = 0;

        for (final Integer updateID : allReplicas.values()) {
            if (updateID > expectedUpdateID) {
                expectedUpdateID = updateID;
            }
        }
        return expectedUpdateID;
    }

    /**
     * Execute a query on the specified database instance by creating a new asynchronous callable executor ({@link Executors},
     * {@link Future}).
     * 
     * <p>
     * This method begins execution of the queries but does not actually return their results (because it is asynchronous). See
     * {@link TableProxy#waitUntilRemoteQueriesFinish(boolean[], List)} for the result.
     * 
     * @param sql
     *            The query to be executed
     * @param transactionName
     *            The name of the transaction in which this query is in.
     * @param replicaToExecuteQueryOn
     *            The database instance where this query will be sent.
     * @param updateID
     * @param isReplicaLocal
     *            Whether this database instance is the local instance, or it is remote.
     * @param parser
     *            The parser to be used to parser the query if it is local.
     * @param executingQueries
     *            The list of queries that have already been sent. The latest query will be added to this list.
     * @param commitOperation
     *            True if this is a COMMIT, false if it is another type of query. If it is false a PREPARE command will be executed to get
     *            ready for the eventual commit.
     * @param tableInfo
     */
    private void executeQueryOnSpecifiedReplica(final String sql, final String transactionName, final DatabaseInstanceWrapper replicaToExecuteQueryOn, final Integer updateID, final boolean isReplicaLocal, final Parser parser, final List<FutureTask<QueryResult>> executingQueries,
                    final boolean commitOperation, final TableInfo tableInfo) {

        final RemoteQueryExecutor qt = new RemoteQueryExecutor(sql, transactionName, replicaToExecuteQueryOn, updateID, parser, isReplicaLocal, commitOperation, tableInfo);

        final FutureTask<QueryResult> future = new FutureTask<QueryResult>(new Callable<QueryResult>() {

            @Override
            public QueryResult call() {

                return qt.executeQuery();
            }
        });

        executingQueries.add(future);
        queryExecutor.execute(future);

    }

    /**
     * Waits on the result of a number of asynchronous queries to be completed and returned.
     * 
     * @param incompleteQueries
     *            The list of tasks currently being executed.
     * @param updatesNeededBeforeCommit
     *            The number of replicas that must be updated for this query to return.
     * @param transactionNameForQuery
     * @param expectedUpdateID
     * @param tableName
     * @return True if everything was executed successfully (a global commit).
     */
    private boolean waitUntilRemoteQueriesFinish(final List<FutureTask<QueryResult>> incompleteQueries, final int updatesNeededBeforeCommit, final String transactionNameForQuery, final int expectedUpdateID, final TableInfo tableName) {

        if (incompleteQueries.size() == 0) { return true; // the commit value has not changed.
        }

        assert sleepTimeWhileWaitingForQueriesToFinish != null : "Sleep time property couldn't be found.";
        final List<FutureTask<QueryResult>> completedQueries = new LinkedList<FutureTask<QueryResult>>();

        /*
         * Wait until all remote queries have been completed.
         */
        while (incompleteQueries.size() > 0 && completedQueries.size() < updatesNeededBeforeCommit) {

            for (int y = 0; y < incompleteQueries.size(); y++) {
                final FutureTask<QueryResult> remoteQuery = incompleteQueries.get(y);

                if (remoteQuery.isDone()) {
                    // If the query is done add it to the list of completed
                    // queries.
                    completedQueries.add(remoteQuery);
                    incompleteQueries.remove(y);
                }
                else {
                    try {

                        Thread.sleep(sleepTimeWhileWaitingForQueriesToFinish);
                    }
                    catch (final InterruptedException e) {
                        e.printStackTrace();
                    }
                    // We could sleep for a time here before checking again.
                }
            }
        }

        boolean globalCommit = true;

        /*
         * All of the queries have now completed. Iterate through these queries and check that they executed successfully.
         */

        final List<CommitResult> recentlyCompletedQueries = new LinkedList<CommitResult>();

        for (final FutureTask<QueryResult> completedQuery : completedQueries) {

            QueryResult asyncResult = null;

            try {
                asyncResult = completedQuery.get();
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to obtain result from asynchronous query. The replica involved will be marked as inactive if this transaction commits.");
                continue;
            }

            assert asyncResult != null : "The result of a completed transaction should never be null."; //$NON-NLS-1$

            if (asyncResult.getException() == null) { // If the query executed successfully.
                final int result = asyncResult.getResult();
                final DatabaseInstanceWrapper url = asyncResult.getWrapper();
                if (result != 0) {
                    // Prepare operation failed at remote machine
                    final CommitResult commitResult = new CommitResult(false, url, asyncResult.getUpdateID(), expectedUpdateID, tableName);
                    recentlyCompletedQueries.add(commitResult);
                    globalCommit = false;
                }
                else {
                    final CommitResult commitResult = new CommitResult(true, url, asyncResult.getUpdateID(), expectedUpdateID, tableName);
                    recentlyCompletedQueries.add(commitResult);
                }
            }
            else {
                final CommitResult commitResult = new CommitResult(true, asyncResult.getWrapper(), asyncResult.getUpdateID(), expectedUpdateID, tableName);
                recentlyCompletedQueries.add(commitResult);

                if (asyncResult.getException() != null) {
                    asyncResult.getException().printStackTrace();
                }
                globalCommit = false;
            }

        }

        database.getAsynchronousQueryManager().addTransaction(transactionNameForQuery, tableName, incompleteQueries, recentlyCompletedQueries, expectedUpdateID);

        return globalCommit;

    }

}
