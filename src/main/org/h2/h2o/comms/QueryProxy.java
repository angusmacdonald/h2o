package org.h2.h2o.comms;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.TwoPhaseCommit;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TransactionNameGenerator;
import org.h2.test.h2o.H2OTest;

import uk.ac.stand.dcs.nds.util.Diagnostic;
import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * A proxy class used to make sending queries to multiple replicas easier. The query only needs to be sent
 * to the query proxy, which handles the rest of the transaction.
 * 
 * <p>Query proxies are created by the data manager for a given table, and indicate permission to perform a given query. The
 * level of this permission is indicated by the type of lock granted (see lockGranted attribute).
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryProxy implements Serializable{

	/**
	 * Generated serial version.
	 */
	private static final long serialVersionUID = -31853777345527026L;

	private LockType lockGranted;

	private String tableName;

	private Set<DatabaseInstanceRemote> allReplicas;

	/**
	 * Proxy for the data manager of this table. Used to release any locks held at the end of the transaction.
	 */
	private DataManagerRemote dataManagerProxy;

	/**
	 * The database instance making the request. This is used to request the lock (i.e. the lock for the given query
	 * is taken out in the name of this database instance.
	 */
	private DatabaseInstanceRemote requestingDatabase;


	/**
	 * @param lockGranted		The type of lock that has been granted
	 * @param tableName			Name of the table that is being used in the query
	 * @param replicaLocations	Proxies for each of the replicas used in the query.
	 * @param dataManager		Proxy for the data manager of the table involved in the query (i.e. tableName).
	 */
	public QueryProxy(LockType lockGranted, String tableName,
			Set<DatabaseInstanceRemote> replicaLocations, DataManagerRemote dataManager, DatabaseInstanceRemote requestingMachine) {
		super();
		this.lockGranted = lockGranted;
		this.tableName = tableName;
		this.allReplicas = replicaLocations;
		this.dataManagerProxy = dataManager;
		this.requestingDatabase = requestingMachine;
	}

	/**
	 * Execute the given SQL update.
	 * @param sql The query to be executed
	 * @param db	Used to obtain proxies for remote database instances.
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {

		if (allReplicas == null || allReplicas.size() == 0){
			try {
				dataManagerProxy.releaseLock(requestingDatabase);
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to release lock - couldn't contact the data manager");
			}
			Diagnostic.traceNoEvent(Diagnostic.FINAL, "No replicas found to perform update: " + sql);
			return 0;
		}

		String transactionName = TransactionNameGenerator.generateName(); 

		SQLException exception = null;

		/*
		 * Whether the transaction should commit or rollback. Defaults to true, but set to false if one
		 * of the PREPARE operations fails - i.e. every replica must be available to commit.
		 */
		boolean globalCommit = true; 

		/*
		 * Whether an individual replica is able to commit. Used to stop ROLLBACK calls being made to unavailable replicas.
		 */
		boolean[] commit = new boolean[allReplicas.size()]; 

		H2OTest.rmiFailure(); //Test code to simulate the failure of DB instances at this point.

		/*
		 * Send the query to each DB instance holding a replica.
		 */
		int i = 0;
		for (TwoPhaseCommit replica: allReplicas){
			try {
				int result = replica.prepare(sql, transactionName);

				if (result != 0) {
					globalCommit = false; // Prepare operation failed at remote machine, so rollback the query everywhere.
					commit[i++] = false;
				} else {
					commit[i++] = true;
				}

				//TODO include more specific exceptions - e..g replica can't commit
			} catch (RemoteException e) {
				e.printStackTrace();
				//ErrorHandling.errorNoEvent("Unable to contact one of the DB instances holding a replica for " + tableName + ".");
				globalCommit = false; // rollback the entire transaction
				commit[i++] = false;
			} catch (SQLException e){
				globalCommit = false; // rollback the entire transaction.
				commit[i++] = false;
				exception = e;
			}
		}

		H2OTest.rmiFailure(); //Test code to simulate the failure of DB instances at this point.

		/*
		 * Commit or rollback the transaction.
		 */
		int result = 0;
		i = 0;
		for (TwoPhaseCommit remoteReplica: allReplicas){
			if (commit[i]){
				try {
					result = remoteReplica.commit(globalCommit, transactionName);
				} catch (RemoteException e) {
					//ErrorHandling.errorNoEvent("Unable to send " + (commit? "commit": "rollback") + " message to remote replica.");

					//TODO this means that a replica has not be updated... yet (?). Should it be removed from the set of 'active' replicas.

					throw new SQLException((globalCommit? "COMMIT": "ROLLBACK") + " failed on a replica because database instance was unavailable.");
				} catch (SQLException e){

					//TODO again, this indicates the failure to commit at one of the replicas. Should it be removed from the set of 'active' replicas.

					throw new SQLException("Unable to send 'commit' to one of the replicas.");
				}
			}

			i++;
		}

		try {
			dataManagerProxy.releaseLock(requestingDatabase);
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to release lock - couldn't contact the data manager");
		}

		/*
		 * If rollback was performed - throw an exception informing requesting party of this.
		 */
		if (!globalCommit){
			if (exception != null){
				throw exception;
			} else {
				throw new SQLException("Couldn't complete update because one or a number of replicas failed.");
			}
		}

		return result;
	}

	/**
	 * Obtain a query proxy for the given table.
	 * @param tableName		Table involved in query.
	 * @param lockType		Lock required for query
	 * @param db			Local database instance - needed to inform DM of: the identity of the requesting machine,
	 * 							and to obtain the data manager for the given table.
	 * @return
	 * @throws SQLException
	 */
	public static QueryProxy getQueryProxy(String tableName, LockType lockType, Database db) throws SQLException {
		return getQueryProxy(db.getDataManager(tableName), lockType, db.getLocalDatabaseInstance());
	}

	/**
	 * Obtain a query proxy for the given table.
	 * @param dataManager
	 * @param requestingDatabase DB making the request.
	 * @return Query proxy for a specific table within H20.
	 * @throws SQLException
	 */
	public static QueryProxy getQueryProxy(DataManagerRemote dataManager, LockType lockType, DatabaseInstanceRemote requestingDatabase) throws SQLException {

		QueryProxy qp = null;

		if (dataManager == null){
			ErrorHandling.errorNoEvent("Data manager proxy was null when requesting table.");
			throw new SQLException("Data manager not found for table.");
		}

		if(requestingDatabase == null){
			System.err.println("Shouldn't happen.");
		}


		try {
			qp = dataManager.requestQueryProxy(lockType, requestingDatabase);
		} catch (RemoteException e) {
			e.printStackTrace();
			throw new SQLException("Unable to obtain query proxy from data manager (remote exception).");
		}

		return qp;
	}

	public LockType getLockGranted(){
		return lockGranted;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return tableName + " ("+ allReplicas.size() + " replicas), with lock '" + lockGranted + "'";
	}

}
