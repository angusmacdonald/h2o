package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.manager.Migratable;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TableInfo;


/**
 * Remote interface for Table Manager instances.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface TableManagerRemote extends H2ORemote, Migratable {

	public QueryProxy getQueryProxy(LockType lockType, DatabaseInstanceRemote databaseInstanceRemote) throws RemoteException, SQLException, MovedException;

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.PersistentManager#addTableInformation(org.h2.h2o.util.DatabaseURL, org.h2.h2o.util.TableInfo)
	 */
	public boolean addTableInformation(DatabaseURL tableManagerURL,
			TableInfo tableDetails) throws RemoteException, MovedException, SQLException;

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.PersistentManager#addReplicaInformation(org.h2.h2o.util.TableInfo)
	 */
	public void addReplicaInformation(TableInfo tableDetails)  throws RemoteException, MovedException, SQLException;

	public void removeReplicaInformation(TableInfo ti)  throws RemoteException, MovedException, SQLException;

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#removeTableManager()
	 */
	public boolean removeTableManager() throws RemoteException, SQLException,
			MovedException;

	/**
	 * Get the location of a single replica for the given table. This is used in creating linked
	 * tables, so the return type is string rather than DatabaseInstanceRemote.
	 * @return Database connection URL for a given remote database.
	 * @throws RemoteException 
	 */
	public String getLocation() throws RemoteException, MovedException;

	/**
	 * Release a lock held by the database instance specified in the parameter. Called at the end of QueryProxy.executeQuery()
	 * to indicate that the transaction has finished (it may have succeeded or failed).
	 * @param requestingDatabase	Database which made the original request. Lock was taken out in its name.
	 * @param updateID The ID given to the update by the Table Manager. It is returned here to confirm execution of this specific transaction.
	 * @param updatedReplicas The set of replicas that were successfully updated by this query.
	 * @throws MovedException 
	 */
	public void releaseLock(DatabaseInstanceRemote requestingDatabase, Set<DatabaseInstanceRemote> updatedReplicas, int updateID) throws RemoteException, MovedException;

	/**
	 * Deconstruct this Table Manager. This is required for testing where a remote reference to a Table Manager may not completely die when
	 * expected - this method should essentially render the Table Manager unusable.
	 */
	public void shutdown() throws RemoteException;

	/**
	 * The name of the schema which this table is in.
	 */
	public String getSchemaName()throws RemoteException;

	/**
	 * The name of the table this Table Manager is responsible for (not including schema name).
	 */
	public String getTableName()throws RemoteException;

	/**
	 * The object responsible for managing the set of replicas this Table Manager maintains.
	 * 
	 * <p>This is called when the Table Manager is being migrated elsewhere, but shouldn't need to be
	 * called anywhere else.
	 */
	public ReplicaManager getReplicaManager() throws RemoteException;

	/**
	 * Get the table set that this table is part of.
	 */
	public int getTableSet() throws RemoteException;

	/**
	 * Build up the state of this Table Manager from the state of another extant manager. Used when migrating the state of the old
	 * manager to this manager.
	 * @param oldTableManager	Extant Table Manager.
	 * @throws MovedException 	Thrown if this Table Manager has already been moved to somewhere else.
	 */
	public void buildTableManagerState(TableManagerRemote oldTableManager) throws RemoteException, MovedException;

	/**
	 * The URL of the database on which this Table Manager is located.
	 */
	public DatabaseURL getDatabaseURL() throws RemoteException;
}
