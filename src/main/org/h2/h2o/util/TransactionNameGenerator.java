package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;

/**
 *	Utility class which generates unique names for transactions. 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TransactionNameGenerator {

	private static int lastNumber = 0; //XXX not the most sophisticated method, but it works.

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
				DatabaseURL dbURL = DatabaseURL.parseURL(requestingDatabase.getConnectionString());
				String hostname = dbURL.getHostname();
				
				if (hostname == null){
					hostname = "local";
				}
				
				part = (dbURL.isTcp()? hostname.replace(".", "") + dbURL.getPort(): "") + dbURL.getDbLocationWithoutSlashes();
			} catch (RemoteException e) {
				part = "UnknownDB";
			}

		}

		return generateFullTransactionName(part);
	}

	private static String generateFullTransactionName(String part){
		String transactionName = "TRANSACTION_";

		transactionName += part;

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
}
