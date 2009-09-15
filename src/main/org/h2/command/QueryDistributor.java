package org.h2.command;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.h2o.comms.DatabaseInstanceRemote;
import org.h2.h2o.comms.TransactionNameGenerator;

import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryDistributor {
	/**
	 * @param remoteReplicaLocations
	 * @throws SQLException
	 */
	public static int sendToAllReplicas(Set<DatabaseInstanceRemote> remoteReplicaLocations, String query, String tableName) throws SQLException {
		int count = 0;
		boolean commit = true; //whether the transaction should commit or rollback.
		
		/*
		 * Send the query to each DB instance holding a replica.
		 */
		for (DatabaseInstanceRemote remoteReplica: remoteReplicaLocations){
			try {
				count = remoteReplica.sendUpdate(query, TransactionNameGenerator.generateName(tableName));

				if (count != 0) commit = false; // Prepare operation failed at remote machine, so rollback the query everywhere.
			} catch (RemoteException e) {
				e.printStackTrace();
				ErrorHandling.errorNoEvent("Unable to contact one of the DB instances holding a replica for " + tableName + ".");
				commit = false; // rollback the entire transaction.
			}

		}

		/*
		 * Commit or rollback the transaction.
		 */
		for (DatabaseInstanceRemote remoteReplica: remoteReplicaLocations){
			try {
				count = remoteReplica.commitQuery(commit, TransactionNameGenerator.generateName(tableName));
			} catch (RemoteException e) {
				ErrorHandling.errorNoEvent("Unable to send " + (commit? "commit": "rollback") + " message to remote replica.");
			}
		}
		
		/*
		 * Rollback was performed - throw an exception informing requesting party of this.
		 */
		if (!commit){
			throw new SQLException("Couldn't complete update because one or a number of replicas failed.");
		}
		
		return count;
	}
}
