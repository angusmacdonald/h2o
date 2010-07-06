package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.test.h2o.H2OTest;

public class QueryThread extends Thread {
	
	private String query;
	
	private String transactionName;
	
	private DatabaseInstanceWrapper databaseWrapper;
	
	int instanceID;
	
	/**
	 * 
	 * @param query
	 * @param transactionName
	 * @param replica
	 * @param instanceID If the transaction has to be rolled back this ID is used to identify the instance in question.
	 */
	public QueryThread(String query, String transactionName, DatabaseInstanceWrapper replica, int instanceID) {
		this.query = query;
		this.transactionName = transactionName;
		this.databaseWrapper = replica;
		this.instanceID = instanceID;
	}

	public QueryResult executeQuery(){
		QueryResult qr = null;
		
				
		try {
			H2OTest.rmiFailure(databaseWrapper);
			int result = databaseWrapper.getDatabaseInstance().prepare(query, transactionName);
			
			qr = new QueryResult(result, instanceID);
			
		} catch (RemoteException e) {
			qr = new QueryResult(new SQLException(e.getMessage()), instanceID);
		} catch (SQLException e) {
			qr = new QueryResult(e, instanceID);
		}
		
		return qr;
	}
	
}
