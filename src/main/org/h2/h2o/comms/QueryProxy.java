package org.h2.h2o.comms;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.engine.Session;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

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
	 * @param remoteReplicaLocations
	 * @throws SQLException
	 */
	public int sendToAllReplicas(String query, Database db) throws SQLException {
		Set<DatabaseInstanceRemote> remoteReplicaLocations = getReplicaLocations(db);

		String transactionName = TransactionNameGenerator.generateName();
		int count = 0;
		boolean commit = true; //whether the transaction should commit or rollback.
		SQLException exception = null;

		/*
		 * Send the query to each DB instance holding a replica.
		 */
		for (DatabaseInstanceRemote remoteReplica: remoteReplicaLocations){
			try {
				count = remoteReplica.prepareQuery(query, transactionName);

				if (count != 0) commit = false; // Prepare operation failed at remote machine, so rollback the query everywhere.
			} catch (RemoteException e) {
				e.printStackTrace();
				ErrorHandling.errorNoEvent("Unable to contact one of the DB instances holding a replica for " + tableName + ".");
				commit = false; // rollback the entire transaction.
			} catch (SQLException e){

				commit = false; // rollback the entire transaction.
				exception = e;
			}


		}

		/*
		 * Commit or rollback the transaction.
		 */
		for (DatabaseInstanceRemote remoteReplica: remoteReplicaLocations){
			try {
				count = remoteReplica.commitQuery(commit, transactionName);
			} catch (RemoteException e) {
				ErrorHandling.errorNoEvent("Unable to send " + (commit? "commit": "rollback") + " message to remote replica.");
			} catch (SQLException e){
				throw new SQLException("Unable to contact data manager.");
			}

		}

		/*
		 * Rollback was performed - throw an exception informing requesting party of this.
		 */
		if (!commit){
			if (exception != null){
				throw exception;
			} else {
				throw new SQLException("Couldn't complete update because one or a number of replicas failed.");
			}
		}

		return count;
	}

	/**
	 * Get the locations of each of the remote replicas for the given table.
	 * @return
	 */
	private Set<DatabaseInstanceRemote> getReplicaLocations(Database db) {

		return db.getDatabaseInstances(replicaLocations);

	}

}
