package org.h2.h2o.comms;

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
	 * @param transactionName	The name to be given to this transaction - must be used again to commit the transaction
	 * @return Result of the prepare - this should never fail in theory, bar some weird disk-based mishap.
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public int prepare(String query, String transactionName)
			throws RemoteException, SQLException;

	/**
	 * Commit a query as per the two phase commit protocol. The query should have previously been prepared via the prepare() method - this
	 * method commits (or aborts) the transaction.
	 * @param transactionName	The name to given to this transaction - used to find which transaction to commit.
	 * @param commit	true if the transaction is to be committed; false for an abort.
	 * @return Result of the commit queries execution. 
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public int commit(boolean commit, String transactionName)
			throws RemoteException, SQLException;

}