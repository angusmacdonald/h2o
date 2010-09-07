/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public class AsynchronousQueryExecutor {

	private Database database;
	
	/**
	 * @param database
	 */
	public AsynchronousQueryExecutor(Database database) {
		this.database = database;
	}

	private static ExecutorService queryExecutor = Executors.newCachedThreadPool(new QueryThreadFactory() {
		public Thread newThread(Runnable r) {
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
	 * @param session
	 *            The session we are in - used to create the query parser.
	 * @param commit
	 *            The array that will be used to store (for each replica) whether the transaction was executed successfully.
	 * @param commitOperation
	 *            True if this is a COMMIT, false if it is another type of query. If it is false a PREPARE command will be executed to get
	 *            ready for the eventual commit.
	 * @return True if everything was executed successfully (a global commit).
	 */
	public boolean executeQuery(String query, String transactionNameForQuery, Map<DatabaseInstanceWrapper, Integer> allReplicas,
			Session session, boolean commitOperation) {

		Parser parser = new Parser(session, true);

		List<FutureTask<QueryResult>> executingQueries = new LinkedList<FutureTask<QueryResult>>();

		int expectedUpdateID = getExpectedUpdateID(allReplicas);
		
		int updatesNeededBeforeCommit = allReplicas.size();
		
		if (session.getDatabase().getDatabaseSettings().get("ASYNCHRONOUS_REPLICATION_ENABLED").equals("true")){
			updatesNeededBeforeCommit = Integer.parseInt(session.getDatabase().getDatabaseSettings().get("ASYNCHRONOUS_REPLICATION_FACTOR"));
		}

		int i = 0;
		for (Entry<DatabaseInstanceWrapper, Integer> replicaToExecuteQueryOn : allReplicas.entrySet()) {

			String localURL = session.getDatabase().getURL().getURL();

			// Decide whether the query is to be executed local or remotely.
			boolean isReplicaLocal = (replicaToExecuteQueryOn == null || localURL.equals(replicaToExecuteQueryOn.getKey().getURL().getURL()));

			// Start execution of queries.
			executeQueryOnSpecifiedReplica(query, transactionNameForQuery, replicaToExecuteQueryOn.getKey(),
					replicaToExecuteQueryOn.getValue(), isReplicaLocal, parser, executingQueries, commitOperation);
			i++;
		}

		// Wait for enough queries to execute, then return the result.
		return waitUntilRemoteQueriesFinish(executingQueries, updatesNeededBeforeCommit, transactionNameForQuery, expectedUpdateID);
	}

	/**
	 * Get the update ID that must be reached on the Table Manager for the update to commit. If it is not reached there has been an
	 * out-of-order query execution and the replica must be removed.
	 * 
	 * @param allReplicas
	 * @return
	 */
	private int getExpectedUpdateID(Map<DatabaseInstanceWrapper, Integer> allReplicas) {
		int expectedUpdateID = 0;

		for (Integer updateID : allReplicas.values()) {
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
	 * {@link QueryProxy#waitUntilRemoteQueriesFinish(boolean[], List)} for the result.
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
	 */
	private void executeQueryOnSpecifiedReplica(String sql, String transactionName, DatabaseInstanceWrapper replicaToExecuteQueryOn,
			Integer updateID, boolean isReplicaLocal, Parser parser, List<FutureTask<QueryResult>> executingQueries, boolean commitOperation) {

		final RemoteQueryExecutor qt = new RemoteQueryExecutor(sql, transactionName, replicaToExecuteQueryOn, updateID, parser,
				isReplicaLocal, commitOperation);

		FutureTask<QueryResult> future = new FutureTask<QueryResult>(new Callable<QueryResult>() {
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
	 * @param remoteQueries
	 *            The list of tasks currently being executed.
	 * @param updatesNeededBeforeCommit
	 *            The number of replicas that must be updated for this query to return.
	 * @param transactionNameForQuery 
	 * @param expectedUpdateID 
	 * @return True if everything was executed successfully (a global commit).
	 */
	private boolean waitUntilRemoteQueriesFinish(List<FutureTask<QueryResult>> remoteQueries,
			int updatesNeededBeforeCommit, String transactionNameForQuery, int expectedUpdateID) {
		if (remoteQueries.size() == 0)
			return true; // the commit value has not changed.

		List<FutureTask<QueryResult>> completedQueries = new LinkedList<FutureTask<QueryResult>>();

		/*
		 * Wait until all remote queries have been completed.
		 */
		while (remoteQueries.size() > 0 && completedQueries.size() < updatesNeededBeforeCommit) {
			for (int y = 0; y < remoteQueries.size(); y++) {
				FutureTask<QueryResult> remoteQuery = remoteQueries.get(y);

				if (remoteQuery.isDone()) {
					// If the query is done add it to the list of completed
					// queries.
					completedQueries.add(remoteQuery);
					remoteQueries.remove(y);
				} else {
					// We could sleep for a time here before checking again.
				}
			}
		}

		boolean globalCommit = true;

		/*
		 * All of the queries have now completed. Iterate through these queries and check that they executed successfully.
		 */
		
		List<CommitResult> recentlyCompletedCommits = new LinkedList<CommitResult>();

		
		
		for (FutureTask<QueryResult> completedQuery : completedQueries) {

			QueryResult asyncResult = null;
			
			try {
				asyncResult = completedQuery.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}

			if (asyncResult.getException() == null) { // If the query executed successfully.
				int result = asyncResult.getResult();
				DatabaseInstanceWrapper url = asyncResult.getWrapper();
				if (result != 0) {
					// Prepare operation failed at remote machine
					CommitResult commitResult = new CommitResult(false, url, asyncResult.getUpdateID(), expectedUpdateID);
					recentlyCompletedCommits.add(commitResult);
					globalCommit = false;
				} else {
					CommitResult commitResult = new CommitResult(true, url, asyncResult.getUpdateID(), expectedUpdateID);
					recentlyCompletedCommits.add(commitResult);
				}
			} else {
				CommitResult commitResult = new CommitResult(true, asyncResult.getWrapper(), asyncResult.getUpdateID(), expectedUpdateID);
				recentlyCompletedCommits.add(commitResult);
				globalCommit = false;
			}
			
		}
		
		database.getAsynchronousQueryManager().addTransaction(transactionNameForQuery, remoteQueries, expectedUpdateID);

		return globalCommit;

	}

}
