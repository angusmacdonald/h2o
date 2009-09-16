package org.h2.h2o.comms;

/**
 *	Utility class which generates unique names for transactions. 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TransactionNameGenerator {

	private static int lastNumber = 0; //XXX not the most sophisticated method, but it works.
	
	/**
	 * Generate a unique name for a new transaction.
	 * @param tableName Name of a table involved in the transaction.
	 * @return
	 */
	public static synchronized String generateName(){
		
		
		return "TRANSACTION_" + lastNumber++;
	}
}
