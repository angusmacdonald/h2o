package org.h2o.db.query.asynchronous;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages the set of updates currently being executed by this database instance. There is one
 * instance of this class per database instance.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class AsynchronousQueryManager {
	
	/**
	 * Map of currently executing transactions.
	 * 
	 * <p>Key: transaction name
	 * <p>Value: transaction object
	 */
	Map <String, Transaction> activeTransactions = new HashMap  <String, Transaction>();
	
	public void addTransaction(Transaction newTransaction) {
		activeTransactions.put(newTransaction.getTransactionID(), newTransaction);
	}
	
	public synchronized Transaction getTransaction(String transactionID) {
		return activeTransactions.get(transactionID);
	}

	/**
	 * Go through all active transactions and check if any have finished executing.
	 */
	public synchronized void checkForCompletion() {
		
		for (Transaction activeTransaction : activeTransactions.values()) {
			activeTransaction.checkForCompletion();
		}
	}

}
