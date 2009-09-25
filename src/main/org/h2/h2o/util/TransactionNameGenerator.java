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
	 * Generate a unique name for a new transaction.
	 * @param requestingDatabase 
	 * @param tableName Name of a table involved in the transaction.
	 * @return
	 */
	public static synchronized String generateName(DatabaseInstanceRemote requestingDatabase){

		DatabaseURL dbURL = null;

		String transactionName = "TRANSACTION_";

		try {
			dbURL = DatabaseURL.parseURL(requestingDatabase.getConnectionString());

			transactionName += (!dbURL.isMem()? dbURL.getHostname().replace(".", "") + dbURL.getPort(): "") + dbURL.getDbLocation().replace("/", "");
		} catch (RemoteException e) {
		}

		return transactionName + lastNumber++;
	}
}
