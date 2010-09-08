package org.h2o.db.query.asynchronous;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public class Transaction {

	private String transactionID;

	/**
	 * The set of all queries being executed as part of this transaction.
	 */
	private List<CommitResult> completedQueries;

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
	public Transaction(String transactionID, List<FutureTask<QueryResult>> executingQueries, List<CommitResult> recentlyCompletedQueries, int expectedUpdateID) {
		this.transactionID = transactionID;
		this.incompleteQueries = executingQueries;
		this.expectedUpdateID = expectedUpdateID;
		this.completedQueries = recentlyCompletedQueries;
		
		if (completedQueries == null){
			completedQueries = new LinkedList<CommitResult>();
		}
	}

	/**
	 * Check whether any active queries have finished executing.
	 */
	public void checkForCompletion() {

		List<FutureTask<QueryResult>> recentlyCompletedQueries = new LinkedList<FutureTask<QueryResult>>();

		/*
		 * Wait until all remote queries have been completed.
		 */
		while (incompleteQueries.size() > 0) {
			for (int y = 0; y < incompleteQueries.size(); y++) {
				FutureTask<QueryResult> incompleteQuery = incompleteQueries.get(y);

				if (incompleteQuery.isDone()) {
					incompleteQueries.remove(y);
					recentlyCompletedQueries.add(incompleteQuery);

					/*
					 * When a query is done it should be added to a local list of 
					 * recently completed queries, and information should be sent to the table
					 * manager *if* the transaction has completed already.
					 */
				}
			}
		}

		/*
		 * 
		 */

		List<CommitResult> recentlyCompletedCommits = new LinkedList<CommitResult>();

		for (FutureTask<QueryResult> completedQuery: recentlyCompletedQueries){

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
				DatabaseInstanceWrapper wrapper = asyncResult.getWrapper();
				if (result != 0) {
					// Prepare operation failed at remote machine
					CommitResult commitResult = new CommitResult(false, wrapper, asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
					recentlyCompletedCommits.add(commitResult);
					
				} else {
					CommitResult commitResult = new CommitResult(true, wrapper, asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
					recentlyCompletedCommits.add(commitResult);
				}

			} else {
				CommitResult commitResult = new CommitResult(true, asyncResult.getWrapper(), asyncResult.getUpdateID(), expectedUpdateID, asyncResult.getTable());
				recentlyCompletedCommits.add(commitResult);
			}

		}


		if (transactionHasCommitted){ //Send all new commits to the table manager.
			commit(recentlyCompletedCommits);
		} else { //Store new commits along with other commits for this transaction.
			completedQueries.addAll(recentlyCompletedCommits);
		}

	}

	/**
	 * Called from within this class when some updates have recently completed, but the transaction has already
	 * been committed. These updates should now be reflected in the Table Manager for the given table.
	 * @param completedUpdates
	 */
	public synchronized void commit(List<CommitResult> completedUpdates){
		/*
		 * TODO Called by the query proxy manager to commit a transaction. All the updated replicas (based on
		 * information in this class) will be committed to the table manager.
		 * 
		 * This method should return some data structure containing the names of updated replicas and
		 * information on their update ID, etc...
		 */
	}
	
	/**
	 * Called by the QueryProxyManager for a transaction when it is committing the transaction. It must send details of the
	 * completed updates to the Table Manager.
	 */
	public synchronized List<CommitResult> getCompletedQueries(){
		return completedQueries;
		
		/*
		 * TODO Called by the query proxy manager to commit a transaction. All the updated replicas (based on
		 * information in this class) will be committed to the table manager.
		 * 
		 * This method should return some data structure containing the names of updated replicas and
		 * information on their update ID, etc...
		 */
	}
	
	

	public String getTransactionID() {
		return transactionID;
	}



	public boolean hasCommitted() {
		return transactionHasCommitted;
	}

	public void addQueries(List<FutureTask<QueryResult>> newIncompleteQueries) {
		incompleteQueries.addAll(newIncompleteQueries);
	}

	public void addCompletedQueries(List<CommitResult> recentlyCompletedQueries) {
		completedQueries.addAll(recentlyCompletedQueries);
	}
}
