package org.h2o.db.query.asynchronous;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.FutureTask;

import org.h2.engine.Database;
import org.h2o.db.id.TableInfo;

/**
 * Manages the set of updates currently being executed by this database instance. There is one
 * instance of this class per database instance.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class AsynchronousQueryManager {
	
	private AsynchronousQueryCheckerThread checkerThread = new AsynchronousQueryCheckerThread(this);
	
	private Database db;
	
	public AsynchronousQueryManager(Database db){
		this.db = db;
		checkerThread.start();
	}
	
	/**
	 * Map of currently executing transactions.
	 * 
	 * <p>Key: transaction name
	 * <p>Value: transaction object
	 */
	Map <String, Transaction> activeTransactions = new HashMap  <String, Transaction>();

	public synchronized void addTransaction(String transactionNameForQuery, TableInfo tableName, List<FutureTask<QueryResult>> incompleteQueries, List<CommitResult> recentlyCompletedQueries, int expectedUpdateID) {
		if (activeTransactions.containsKey(transactionNameForQuery)){
			Transaction existingTransaction = activeTransactions.get(transactionNameForQuery);
			existingTransaction.addQueries(incompleteQueries);
			existingTransaction.addCompletedQueries(recentlyCompletedQueries);
			activeTransactions.put(transactionNameForQuery, existingTransaction);
		} else {
			Transaction newTransaction = new Transaction(transactionNameForQuery, incompleteQueries, recentlyCompletedQueries, expectedUpdateID);
			
			activeTransactions.put(transactionNameForQuery, newTransaction);
		}
	}
	
	public synchronized Transaction getTransaction(String transactionID) {
		return activeTransactions.get(transactionID);
	}

	/**
	 * Go through all active transactions and check if any have finished executing.
	 */
	public synchronized void checkForCompletion() {
		
		List<String> finishedTransactions = new LinkedList<String>();
		
		for (Entry<String, Transaction> activeTransaction : activeTransactions.entrySet()) {
			boolean finished = activeTransaction.getValue().checkForCompletion(db);
			
			if (finished){
				finishedTransactions.add(activeTransaction.getKey());
			}
		}
		
		for (String transactionName : finishedTransactions) {
			activeTransactions.remove(transactionName);
		}
	}


}
