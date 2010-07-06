package org.h2.h2o.comms;

import java.sql.SQLException;

public class QueryResult {
	private int result;
	
	private SQLException exception = null;

	private int instanceID;
	
	public QueryResult(int result, int instanceID) {
		this.result = result;
		this.instanceID = instanceID;
	}

	public QueryResult(SQLException exception, int instanceID) {
		this.exception = exception;
		this.instanceID = instanceID;
	}
	
	public int getResult() {
		return result;
	}

	public SQLException getException() {
		return exception;
	}
	
	public int getInstanceID(){
		return instanceID;
	}
	
	
}
