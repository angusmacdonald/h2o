package org.h2.h2o.comms;

import java.rmi.RemoteException;

import org.h2.command.Prepared;

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
	
	public DatabaseInstance(String databaseConnectionString){
		this.databaseConnectionString = databaseConnectionString;
	}
	
	/* (non-Javadoc)
	 * @see org.h2.command.dm.DatabaseInstanceRemote#executeUpdate(org.h2.command.Prepared)
	 */
	public int executeUpdate(String query) throws RemoteException{
		System.out.println("Update received.");
		return 0;
		
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
