package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DatabaseInstanceRemote extends H2ORemote  {

	/**
	 * Send an update query to be sent to the given machine and parsed, and executed, but not
	 * committed. The {@link=commitQuery()} method must then be called to complete or abort the query.
	 * @param query	SQL query to be executed
	 * @param transactionName	The name to be given to this transaction - must be used again to commit the transaction
	 * @return
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public int prepareQuery(String query, String transactionName) throws RemoteException, SQLException;
	
	/**
	 * 
	 * @return
	 * @param transactionName	The name to given to this transaction - used to find which transaction to commit.
	 * @param commit	true if the transaction is to be committed; false for an abort.
	 * @param string 
	 * @throws RemoteException
	 * @throws SQLException 
	 */
	public int commitQuery(boolean commit, String string) throws RemoteException, SQLException;

}