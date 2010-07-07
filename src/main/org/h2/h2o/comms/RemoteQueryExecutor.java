package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.test.h2o.H2OTest;

public class RemoteQueryExecutor extends Thread {
	
	private String query;
	
	private String transactionName;
	
	private DatabaseInstanceWrapper databaseWrapper;
	
	int instanceID;

	private boolean local;

	private Parser parser;
	
	/**
	 * 
	 * @param query
	 * @param transactionName
	 * @param replica
	 * @param instanceID If the transaction has to be rolled back this ID is used to identify the instance in question.
	 * @param parser Only used to execute the transaction if local is true.
	 * @param local	Whether the transaction is to be executed locally, or sent remotely.
	 */
	public RemoteQueryExecutor(String query, String transactionName, DatabaseInstanceWrapper replica, int instanceID, Parser parser, boolean local) {
		this.query = query;
		this.transactionName = transactionName;
		this.databaseWrapper = replica;
		this.instanceID = instanceID;
		this.parser = parser;
		this.local = local;
	}

	public QueryResult executeQuery(){
		if (local){
			return executeLocal();
		} else {
			return executeRemote();
		}
	}

	private QueryResult executeLocal() {
		QueryResult qr = null;
		try {
			Command command = parser.prepareCommand(query);

			/*
			 * If called from here executeUpdate should always be told the query is part of a larger transaction, because it
			 * was remotely initiated and consequently needs to wait for the remote machine to commit.
			 */
			command.executeUpdate(true);
			command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
			int result = command.executeUpdate();
			
			qr = new QueryResult(result, instanceID);
			
		} catch (SQLException e) {
			qr = new QueryResult(e, instanceID);
		}
		
		return qr;
	}
	
	
	private QueryResult executeRemote() {
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
