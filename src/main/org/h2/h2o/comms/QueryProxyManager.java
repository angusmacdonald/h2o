package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TransactionNameGenerator;
import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * Manages query proxies where multiple instances are required in a single transaction.
 * 
 * <p>Situations where this is important, include: where multiple tables are on the same machines, and where a table is accessed
 * by multiple queries (meaning locks only need to be taken out once).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class QueryProxyManager {

	private String transactionName; 

	private DatabaseInstanceRemote localDatabase;

	private Parser parser;

	private Set<DatabaseInstanceRemote> allReplicas;

	private Set<DataManagerRemote> dataManagers;

	private DatabaseInstanceRemote requestingDatabase;

	private Map<String, QueryProxy> queryProxies;

	private Command prepareCommand = null; 



	/**
	 * The update ID for this transaction. This is the highest update ID returned by the query proxies held in this manager.
	 */
	private int updateID = 0;

	/**
	 * 
	 * @param db
	 * @param session The session on which this set of queries is being executed. This must be the same session as the one used to
	 * execute the set of queries, otherwise commit won't work correctly (it won't be able to unlock anything).
	 */
	public QueryProxyManager(Database db, Session session){

		this.localDatabase = db.getLocalDatabaseInstance();

		this.transactionName = TransactionNameGenerator.generateName(this.localDatabase); 

		this.parser = new Parser(session, true);

		this.allReplicas = new HashSet<DatabaseInstanceRemote>();
		this.dataManagers = new HashSet<DataManagerRemote>();

		this.requestingDatabase = db.getLocalDatabaseInstance();

		this.queryProxies = new HashMap<String, QueryProxy>();


	}

	/**
	 * Adds a proxy object for one of the SQL statements being executed in this transaction. The proxy object contains details
	 * of any locks this database instance now holds for a given table.
	 * @param proxy	Proxy for a particular table.
	 */
	public void addProxy(QueryProxy proxy) throws SQLException {

		if (!hasLock(proxy)){
			throw new SQLException("Table already locked. Cannot perform query.");
		}

		if (proxy.getReplicaLocations() != null && proxy.getReplicaLocations().size() > 0){
			allReplicas.addAll(proxy.getReplicaLocations()); //XXX this method is weird. might not work.
		} else {
			/*
			 * Adds the local database to the set of databases holding something relevent to the query, IF the set is currently empty. 
			 * Executed if no replica location was specified by the query proxy, which will happen on queries which
			 * don't involve a particular table (these are always local anyway).
			 */
			allReplicas.add(parser.getSession().getDatabase().getLocalDatabaseInstance());
		}

		if (proxy.getDataManagerLocation() != null){
			dataManagers.add(proxy.getDataManagerLocation());
		}

		if (proxy.getUpdateID() > this.updateID){ // the update ID should be the highest of all the proxy update IDs
			this.updateID = proxy.getUpdateID();
		}

		queryProxies.put(proxy.getTableName(), proxy);
	}

	/**
	 *  Tests whether any locks are already held for the given table, either by the new proxy, or
	 *  by the manager itself.
	 * @param proxy	New proxy.
	 * @return true if locks are already held by one of the proxies; otherwise false.
	 */
	public boolean hasLock(QueryProxy proxy) {
		if (proxy.getLockGranted() != LockType.NONE)
			return true; //this proxy already holds the required lock 

		//The proxy doesn't hold the lock - does the manager already have it?
		if (dataManagers.contains(proxy.getDataManagerLocation())){

			proxy.setLockType(LockType.WRITE); //TODO fix hardcoded lock type.
			return true; //XXX this check isn't perfect, but will do for now.
		} else {
			return false;
		}
	}

	/**
	 * Commit the transaction being run through this proxy manager. Involves contacting each machine taking part in the
	 * transaction and sending a commit for the correct transaction name.
	 * @param commit True if the transaction is to be committed. False if the transaction should be rolled back.
	 * @throws SQLException 
	 */
	public void commit(boolean commit) throws SQLException {
		/*
		 * The set of replicas that were updated. This is returned to the DM when locks are released.
		 */
		Set<DatabaseInstanceRemote> updatedReplicas = new HashSet<DatabaseInstanceRemote>();

		/*
		 * Commit or rollback the transaction.
		 */
		for (DatabaseInstanceRemote remoteReplica: allReplicas){

			boolean actionSuccessful;

			if (remoteReplica == localDatabase){
				//Perform commit locally.
				actionSuccessful = commitLocal(commit);
			} else { //Perform commit via RMI.		 
				actionSuccessful = commitRemote(commit, remoteReplica);
			}

			if (actionSuccessful && commit) updatedReplicas.add(remoteReplica);
		}

		endTransaction(updatedReplicas);

		/*
		 * If rollback was performed - throw an exception informing requesting party of this.
		 */
		//		if (!globalCommit){
		//			if (exception != null){
		//				throw exception;
		//			} else {
		//				throw new SQLException("Couldn't complete update because one or a number of replicas failed.");
		//			}
		//		}

		//XXX not throwing any exceptions at this point because the system is coming to a point where the asynchronous updates are acceptable.
	}

	/**
	 * Commit the transaction on the local machine.
	 * @param commit			Whether to commit or rollback (true if commit)
	 * @return	true if the commit was successful. False if it wasn't, or if it was a rollback.
	 * @throws SQLException 
	 */
	private boolean commitLocal(boolean commit) throws SQLException {
		prepare();

		Command commitCommand = parser.prepareCommand((commit? "COMMIT": "ROLLBACK") + " TRANSACTION " + transactionName);
		int result = commitCommand.executeUpdate();

		return (result == 0);
	}

	/**
	 * Commit the transaction by calling a remote replica and sending the COMMIT transaction command.
	 * @param commit			Whether to commit or rollback (true if commit)
	 * @param remoteReplica		The location to which the commit is being sent.
	 * @return	true if the commit was successful. False if it wasn't, or if it was a rollback.
	 * @throws SQLException
	 */
	private boolean commitRemote(boolean commit,
			DatabaseInstanceRemote remoteReplica) throws SQLException {
		try {
			int result = remoteReplica.commit(commit, transactionName);

			return (result == 0);
		} catch (RemoteException e) {
			//ErrorHandling.errorNoEvent("Unable to send " + (commit? "commit": "rollback") + " message to remote replica.");

			//TODO this means that a replica has not be updated... yet (?). Should it be removed from the set of 'active' replicas.

			//XXX not sure if you want to do the following line with asynchronous updates.
			throw new SQLException((commit? "COMMIT": "ROLLBACK") + " failed on a replica because database instance was unavailable.");
		} catch (SQLException e){
			//This replica wasn't added to the set of 'updated replicas' so the query doesn't need to be completely aborted.
			e.printStackTrace();
			ErrorHandling.errorNoEvent("Unable to send 'commit' to one of the replicas.");

		}

		return false;
	}

	/**
	 * Name of the transaction assigned at the start.
	 * @return
	 */
	public String getTransactionName() {
		return transactionName;
	}

	/**
	 * Release locks for every table that is part of this update. This also updates the information on
	 * which replicas were updated (which are currently active), hence the parameter
	 * @param updatedReplicas The set of replicas which were updated. This is NOT used to release locks, but to update the 
	 * data managers state on which replicas are up-to-date.
	 */
	public void endTransaction(Set<DatabaseInstanceRemote> updatedReplicas) { 
		try {
			for (DataManagerRemote dataManagerProxy: dataManagers){
				dataManagerProxy.releaseLock(requestingDatabase, updatedReplicas, updateID);
			}
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to release lock - couldn't contact the data manager");
		}
	}

	/**
	 * @param table
	 * @return
	 */
	public QueryProxy getQueryProxy(String tableName) {
		return queryProxies.get(tableName);
	}

	/**
	 * Prepare a transaction to be committed. This is only called locally.
	 * @throws SQLException 
	 */
	public void prepare() throws SQLException {
		if (prepareCommand == null){
			prepareCommand = parser.prepareCommand("PREPARE COMMIT " + transactionName);
		}

		prepareCommand.executeUpdate();
	}


}