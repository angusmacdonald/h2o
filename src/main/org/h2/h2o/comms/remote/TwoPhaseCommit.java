package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * RMI interface for H2O's two phase commit functionality.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface TwoPhaseCommit {

	/**
	 * Prepare a query as per the two phase commit protocol. The query will be prepared on the given database instance, but will only
	 * be committed when the commit operation is called.
	 * @param query	SQL query to be executed
	 * @param transactionName	The name to be given to this transaction - must be used again to commit the transaction.
	 * @param commitOperation	True if this is a COMMIT, false if it is another type of query. If it is false a PREPARE command will
	 * be executed to get ready for the eventual commit.
	 * @return Result of the prepare - this should never fail in theory, bar some weird disk-based mishap.
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public int execute(String query, String transactionName, boolean commitOperation)
			throws RemoteException, SQLException;

	/**
	 * Prepare the given machine to commit a set of queries that have already been executed. 
	 * @param transactionName	The name to be given to this transaction - must be used again to commit the transaction.
	 * @return Result of the prepare - this should never fail in theory, bar some weird disk-based mishap.
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public int prepare(String transactionName) 
			throws RemoteException, SQLException;
}