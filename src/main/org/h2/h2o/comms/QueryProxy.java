package org.h2.h2o.comms;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.engine.Session;

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
	
	private Set<String> replicaLocations;
	

	/**
	 * @param lockGranted
	 * @param tableName
	 * @param replicaStrings
	 * @param basicQuery
	 */
	public QueryProxy(LockType lockGranted, String tableName,
			Set<String> replicaStrings) {
		super();
		this.lockGranted = lockGranted;
		this.tableName = tableName;
		this.replicaLocations = replicaStrings;
	}


	/**
	 * Get the locations of each of the remote replicas for the given table.
	 * @return
	 */
	public Set<DatabaseInstanceRemote> getReplicaLocations(Database db) {

		return db.getDatabaseInstances(replicaLocations);
		
	}
	
	/*
	 * 
	 * The query will be parsed locally, and this proxy will be used to send it to each of the given DatabaseLocations. 2PC must be used here.
	 */
}
