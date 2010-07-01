package org.h2.h2o.util;

import static org.junit.Assert.assertNotNull;

import java.rmi.RemoteException;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.junit.Test;

/**
 *Utility class which generates unique names for transactions. 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TransactionNameGenerator {

	private static long lastNumber = 0;

	/**
	 * Generate a unique name for a new transaction based on the identity of the requesting database.
	 * @param requestingDatabase 	Proxy representing the database making the request.
	 * @return
	 */
	public static synchronized String generateName(DatabaseInstanceRemote requestingDatabase){

		String part = "";
		
		if (requestingDatabase == null){
			part = "UnknownDB";
		} else {

			try {
				DatabaseURL dbURL = requestingDatabase.getConnectionURL();
				String hostname = dbURL.getHostname();
				
				if (hostname == null){
					hostname = "local";
				}
				
				part = (dbURL.isTcp()? hostname.replace(".", "") + dbURL.getPort(): "") + dbURL.getDbLocationWithoutIllegalCharacters();
			} catch (RemoteException e) {
				part = "UnknownDB";
			}

		}

		return generateFullTransactionName(part);
	}

	private static String generateFullTransactionName(String part){
		String transactionName = "TRANSACTION_";

		transactionName += part;

		if (lastNumber == Long.MAX_VALUE){
			lastNumber = -1;
		}
		
		return (transactionName + "_" + lastNumber++).toUpperCase();
	}

	/**
	 * Generate a unique name for a new local transaction. Transactions spanning multiple databases should used the other
	 * method.
	 * @param requestingDatabase 
	 * @param tableName Name of a table involved in the transaction.
	 * @return
	 */
	public static String generateName(String string) {
		return generateFullTransactionName(string);
	}
	
	/**
	 * Test that transaction names are correctly generated even when the number of transactions
	 * exceeds the maximum allowed long value.
	 */
	@Test
	public void testGeneration(){
		lastNumber = Long.MAX_VALUE - 1000;
		
		for (long i = 0; i < 2000; i++){
			generateName("test");
		}
	}
	
	/**
	 * Check that a null Database Instance parameter is handled without error.
	 */
	@Test
	public void nullCheck(){
		assertNotNull(generateName((DatabaseInstanceRemote)null));
	}
	
	/**
	 * Check that a null string parameter is handled without error.
	 */
	@Test
	public void nullCheck2(){
		assertNotNull(generateName((String)null));
	}
}
