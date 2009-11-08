package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * Proxy class exposed via RMI, allowing semi-parsed queries to be sent to remote replicas for execution.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstance implements DatabaseInstanceRemote {

	/**
	 * The JDBC connection string for this database.
	 */
	private String databaseConnectionString;

	private Parser parser;

	private Session session;

	public DatabaseInstance(String databaseConnectionString, Session session){
		this.databaseConnectionString = databaseConnectionString;

		this.parser = new Parser(session, true);
		this.session = session;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.dm.DatabaseInstanceRemote#executeUpdate(org.h2.command.Prepared)
	 */
	public int prepare(String query, String transactionName) throws RemoteException, SQLException{
		if (query == null){
			ErrorHandling.hardError("Shouldn't happen.");
		}

		//session.setAutoCommit(false); //TODO auto-commit shouldn't be set false.true for each transaction.
		Command command = parser.prepareCommand(query);
		
		/*
		 * If called from here executeUpdate should always be told the query is part of a larger transaction, because it
		 * was remotely initiated and consequently needs to wait for the remote machine to commit.
		 */
		command.executeUpdate(true);
	
		command.close();

		return prepare(transactionName);
	}
	
	public int prepare(String transactionName) throws RemoteException, SQLException{
		
		if (session.getAutoCommit()){
			ErrorHandling.hardError("Shouldn't happen.");
		}
			
		Command command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
		return command.executeUpdate();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#commitQuery(boolean)
	 */
	@Override
	public int commit(boolean commit, String transactionName) throws RemoteException, SQLException {
		Command command = parser.prepareCommand((commit? "commit": "rollback") + " TRANSACTION " + transactionName);
		int result = command.executeUpdate();

		//session.setAutoCommit(true); //TODO auto-commit shouldn't be set false.true for each transaction.
		return result;
	}

	@Override
	@Deprecated
	/**
	 * @Deprecated Because it doesn't currently pass in a transaction name. Do we generate one is this method???
	 */
	public int executeUpdate(QueryProxy queryProxy, String sql) throws RemoteException,
			SQLException {
		/*
		 * TODO eventually this method may do a lot more - e.g. the query may only be run here, and asynchronously run elsewhere. Another
		 * overloaded version of the method may take only the query string and be required to obtain the queryProxy seperately. 
		 */
		return queryProxy.executeUpdate(sql, null, session);
	}
	
	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#testAvailability()
	 */
	@Override
	public void testAvailability() throws RemoteException {
		//Does Nothing
	}

	public String getName(){
		return databaseConnectionString;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#getConnectionString()
	 */
	@Override
	public String getConnectionString() throws RemoteException {
		return session.getDatabase().getOriginalDatabaseURL();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((databaseConnectionString == null) ? 0
						: databaseConnectionString.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseInstance other = (DatabaseInstance) obj;
		if (databaseConnectionString == null) {
			if (other.databaseConnectionString != null)
				return false;
		} else if (!databaseConnectionString
				.equals(other.databaseConnectionString))
			return false;
		return true;
	}
}
