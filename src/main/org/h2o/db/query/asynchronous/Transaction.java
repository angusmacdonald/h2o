package org.h2o.db.query.asynchronous;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.h2o.db.id.DatabaseURL;

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

	/**
	 * @param transactionID
	 * @param executingQueries
	 */
	public Transaction(String transactionID, List<FutureTask<QueryResult>> executingQueries) {
		this.transactionID = transactionID;
		this.incompleteQueries = executingQueries;
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
				DatabaseURL url = asyncResult.getURL();
				if (result != 0) {
					// Prepare operation failed at remote machine
					CommitResult commitResult = new CommitResult(false, url, asyncResult.getUpdateID());
					recentlyCompletedCommits.add(commitResult);
					
				} else {
					CommitResult commitResult = new CommitResult(true, url, asyncResult.getUpdateID());
					recentlyCompletedCommits.add(commitResult);
				}

			} else {
				CommitResult commitResult = new CommitResult(true, asyncResult.getURL(), asyncResult.getUpdateID());
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
	 * 
	 * @param completedUpdates
	 */
	public void commit(List<CommitResult> completedUpdates){
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
}
