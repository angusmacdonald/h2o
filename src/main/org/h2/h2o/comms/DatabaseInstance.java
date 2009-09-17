package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Session;

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
			System.err.println("Shouldn't happen.");
		}

		session.setAutoCommit(false); //TODO auto-commit shouldn't be set false.true for each transaction.
		Command command = parser.prepareCommand(query);
		command.executeUpdate();
	
		command.close();

		command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
		return command.executeUpdate();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DatabaseInstanceRemote#commitQuery(boolean)
	 */
	@Override
	public int commit(boolean commit, String transactionName) throws RemoteException, SQLException {
		Command command = parser.prepareCommand((commit? "commit": "rollback") + " TRANSACTION " + transactionName);
		int result = command.executeUpdate();

		session.setAutoCommit(true); //TODO auto-commit shouldn't be set false.true for each transaction.
		return result;
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



}
