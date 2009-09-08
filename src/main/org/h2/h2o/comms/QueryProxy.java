package org.h2.h2o.comms;

import java.io.Serializable;
import java.util.Set;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryProxy implements Serializable{

	/**
	 * Generated serial version.
	 */
	private static final long serialVersionUID = -31853777345527026L;

	public static enum LockType { READ, WRITE, NONE };
	
	private LockType lockGranted;
	
	private String tableName;
	
	private Set<DatabaseInstanceRemote> replicaLocations;
	

	/**
	 * @param lockGranted
	 * @param tableName
	 * @param replicaLocations
	 * @param basicQuery
	 */
	public QueryProxy(LockType lockGranted, String tableName,
			Set<DatabaseInstanceRemote> replicaLocations) {
		super();
		this.lockGranted = lockGranted;
		this.tableName = tableName;
		this.replicaLocations = replicaLocations;
	}
	
	/*
	 * 
	 * The query will be parsed locally, and this proxy will be used to send it to each of the given DatabaseLocations. 2PC must be used here.
	 */
}
