package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.dml.Insert;
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

	public DatabaseInstance(String databaseConnectionString, Session session){
		this.databaseConnectionString = databaseConnectionString;

		this.parser = new Parser(session, true);
	}

	/* (non-Javadoc)
	 * @see org.h2.command.dm.DatabaseInstanceRemote#executeUpdate(org.h2.command.Prepared)
	 */
	public int executeUpdate(String query) throws RemoteException{
		System.out.println("Update: " + query);

		if (query == null){
			System.err.println("Shouldn't happen.");
		}

		int result = -1;
		try {
			Command command = parser.prepareCommand(query);
			result = command.executeUpdate();

			command.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

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
