package org.h2.h2o.comms;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.TableManager;
import org.h2.h2o.util.LockType;
import org.h2.table.Table;
import org.h2.test.h2o.H2OTest;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * A proxy class used to make sending queries to multiple replicas easier. The query only needs to be sent
 * to the query proxy, which handles the rest of the transaction.
 * 
 * <p>Query proxies are created by the Table Manager for a given table, and indicate permission to perform a given query. The
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

	private Set<DatabaseInstanceWrapper> allReplicas;

	/**
	 * Proxy for the Table Manager of this table. Used to release any locks held at the end of the transaction.
	 */
	private TableManagerRemote tableManager; // changed by al - this is either a local guy or an RMI reference

	/**
	 * The database instance making the request. This is used to request the lock (i.e. the lock for the given query
	 * is taken out in the name of this database instance.
	 */
	private DatabaseInstanceWrapper requestingDatabase;

	/**
	 * ID assigned to this update by the Table Manager. This is returned to inform the Table Manager what replicas were updated.
	 */
	private int updateID;

	/**
	 * The type of lock that was requested. It may not have been granted
	 * @see #lockGranted
	 */
	private LockType lockRequested;


	/**
	 * @param lockGranted		The type of lock that has been granted
	 * @param tableName			Name of the table that is being used in the query
	 * @param replicaLocations	Proxies for each of the replicas used in the query.
	 * @param tableManager		Proxy for the Table Manager of the table involved in the query (i.e. tableName).
	 * @param updateID 			ID given to this update.
	 */
	public QueryProxy(LockType lockGranted, String tableName,
			Set<DatabaseInstanceWrapper> replicaLocations, TableManager tableManager, 
			DatabaseInstanceWrapper requestingMachine, int updateID, LockType lockRequested) {
		this.lockGranted = lockGranted;
		this.lockRequested = lockRequested;
		this.tableName = tableName;
		this.allReplicas = replicaLocations;
		this.tableManager = tableManager;
		this.requestingDatabase = requestingMachine;
		this.updateID = updateID;
	}

	/**
	 * Creates a dummy query proxy.
	 * @see #getDummyQueryProxy(DatabaseInstanceRemote)
	 * @param localDatabaseInstance
	 */
	public QueryProxy(DatabaseInstanceWrapper localDatabaseInstance) {
		this.lockGranted = LockType.WRITE;
		this.allReplicas = new HashSet<DatabaseInstanceWrapper>();
		if (localDatabaseInstance != null){ //true when the DB hasn't started yet (management DB, etc.)
			this.allReplicas.add(localDatabaseInstance);
		}
		this.requestingDatabase = localDatabaseInstance;
	}

	/**
	 * Execute the given SQL update.
	 * @param query The query to be executed
	 * @param db	Used to obtain proxies for remote database instances.
	 * @throws SQLException
	 */
	public int executeUpdate(String query, String transactionNameForQuery, Session session) throws SQLException {

		if (lockRequested == LockType.CREATE && (allReplicas == null || allReplicas.size() == 0)){
			/*
			 * If we don't knwo of any replicas and this is a CREATE TABLE statement then we just run the query on the local DB instance.
			 */
			this.allReplicas = new HashSet<DatabaseInstanceWrapper>();
			this.allReplicas.add(requestingDatabase);
		} else if (allReplicas == null || allReplicas.size() == 0){
			/*
			 * If there are no replicas on which to execute the query.
			 */
			try {
				tableManager.releaseLock(requestingDatabase, null, updateID);
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to release lock - couldn't contact the Table Manager");
			} catch (MovedException e) {
				ErrorHandling.hardError("This should never happen at this point. The migrating machine should have a lock taken out.");
			}
			throw new SQLException("No replicas found to perform update: " + query);
		}

		/*
		 * Whether an individual replica is able to commit. Used to stop ROLLBACK calls being made to unavailable replicas.
		 */
		boolean[] commit = new boolean[allReplicas.size()];
		
		/*
		 * Execute the query. Send the query to each DB instance holding a replica.
		 */
		
		AsynchronousQueryExecutor queryExecutor = new AsynchronousQueryExecutor();
		boolean globalCommit = queryExecutor.executeQuery(query, transactionNameForQuery, allReplicas, session, commit);

		H2OTest.rmiFailure(); //Test code to simulate the failure of DB instances at this point.

		if (!globalCommit){
			throw new SQLException("Commit failed on one or more replicas. The query will be rolled back.");
		}

		return 0;
	}

	/**
	 * Obtain a query proxy for the given table.
	 * @param tableName		Table involved in query.
	 * @param lockType		Lock required for query
	 * @param db			Local database instance - needed to inform DM of: the identity of the requesting machine,
	 * 							and to obtain the Table Manager for the given table.
	 * @return
	 * @throws SQLException
	 */
	public static QueryProxy getQueryProxyAndLock(Table table, LockType lockType, Database db) throws SQLException {
		if (table != null){
			return getQueryProxyAndLock(db, table.getFullName(), lockType, db.getLocalDatabaseInstanceInWrapper());
		} else {
			return getDummyQueryProxy(db.getLocalDatabaseInstanceInWrapper());
		}
	}

	/**
	 * Returns a dummy query proxy which indicates that it is possible to execute the query and lists the local (requesting)
	 * database as the only replica. Used in cases where a query won't have to be propagated, or where no particular table is 
	 * specified in the query (e.g. create schema), and so it isn't possible to lock on a particular table.
	 * @param localDatabaseInstance
	 * @return
	 */
	public static QueryProxy getDummyQueryProxy(
			DatabaseInstanceWrapper localDatabaseInstance) {
		return new QueryProxy(localDatabaseInstance);
	}


	/**
	 * Obtain a query proxy for the given table.
	 * @param tableManager
	 * @param requestingDatabase DB making the request.
	 * @return Query proxy for a specific table within H20.
	 * @throws SQLException
	 */
	public static QueryProxy getQueryProxyAndLock(TableManagerRemote tableManager, String tableName, Database db, LockType lockType, DatabaseInstanceWrapper requestingDatabase) throws SQLException {


		if(requestingDatabase == null){
			ErrorHandling.hardError("A requesting database must be specified.");
		}

		try {
			QueryProxy queryProxy = null;
			try{
				queryProxy = tableManager.getQueryProxy(lockType, requestingDatabase);
			} catch(MovedException e){
				//Get an uncached Table Manager from the System Table
				tableManager = db.getSystemTableReference().lookup(tableName, false);

				queryProxy = tableManager.getQueryProxy(lockType, requestingDatabase);
			}
			return queryProxy;
		} catch (java.rmi.NoSuchObjectException e) {
			e.printStackTrace();
			throw new SQLException("Table Manager could not be accessed. It may not have been exported to RMI correctly.");
		} catch (RemoteException e) {
			e.printStackTrace();
			throw new SQLException("Table Manager could not be accessed. The table is unavailable until the Table Manager is reactivated.");
		} catch (MovedException e) {
			throw new SQLException("Table Manager has moved and can't be accessed at this location.");
		}
	}

	public static QueryProxy getQueryProxyAndLock(Database db, String tableName, LockType lockType, DatabaseInstanceWrapper requestingDatabase) throws SQLException {
		TableManagerRemote tableManager = db.getSystemTableReference().lookup(tableName, true);

		if (tableManager == null){
			ErrorHandling.errorNoEvent("Table Manager proxy was null when requesting table.");
			db.getSystemTableReference();
			throw new SQLException("Table Manager not found for table.");
		}

		return getQueryProxyAndLock(tableManager, tableName, db, lockType, requestingDatabase);
	}

	public LockType getLockGranted(){
		return lockGranted;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		if (tableManager == null){
			/*
			 * This is a dummy proxy.
			 */

			return "Dummy Query Proxy: Used for local query involving a system table or another entity entirely.";
		}

		String plural = (allReplicas.size() == 1)? "": "s";
		return tableName + " ("+ allReplicas.size() + " replica" + plural + "), with lock '" + lockGranted + "'";
	}

	/**
	 * @return
	 */
	public Set<DatabaseInstanceWrapper> getReplicaLocations() {
		return allReplicas;
	}
	
	/**
	 * @return
	 */
	public Set<DatabaseInstanceWrapper> getRemoteReplicaLocations() {
		
		Set<DatabaseInstanceWrapper> remoteReplicas = new HashSet<DatabaseInstanceWrapper>(allReplicas);
		boolean removed = remoteReplicas.remove(requestingDatabase);
		
		if (!removed) ErrorHandling.errorNoEvent("Tried to remove the local replica from the set of all replicas, but failed. Possibly equality check problem.");
		
		return remoteReplicas;
	}
	
	/**
	 * @return
	 */
	public TableManagerRemote getTableManager() { 
		return tableManager;
	}

	/**
	 * @return
	 */
	public int getUpdateID() {
		return updateID;
	}

	/**
	 * @param write
	 */
	public void setLockType(LockType write) {
		this.lockGranted = write;
	}

	/**
	 * @return the requestingDatabase
	 */
	public DatabaseInstanceWrapper getRequestingDatabase() {
		return requestingDatabase;
	}


	/**
	 * Checks whether there is only one database in the system. Currently used in CreateReplica to ensure the system
	 * doesn't try to create a replica of something on the same instance.
	 * @return
	 */
	public boolean isSingleDatabase(DatabaseInstanceRemote localDatabase) {
		return (allReplicas != null && allReplicas.size() == 1) && getRequestingDatabase() == localDatabase;
	}

	/**
	 * Name of the table this proxy holds locks for.
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return
	 */
	public int getNumberOfReplicas() {
		return (allReplicas == null)? 0: allReplicas.size();
	}




}
