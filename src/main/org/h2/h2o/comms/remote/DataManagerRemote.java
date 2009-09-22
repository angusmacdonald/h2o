package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.util.LockType;


/**
 * Remote interface for data manager instances.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DataManagerRemote extends H2ORemote {

	public QueryProxy requestQueryProxy(LockType lockType, DatabaseInstanceRemote databaseInstanceRemote) throws RemoteException, SQLException;

	/**
	 * Inform the data manager that a new replica has been created for the given table.
	 * 
	 * @param tableName				Name of the table being added.
	 * @param modificationId		Modification ID of the table.
	 * @param databaseLocation		Location of the table (the database it is stored in)
	 * @param tableType				Type of the table (e.g. Linked, View, Table).
	 * @param localMachineAddress	Address through which the DB is contactable.
	 * @param localMachinePort		Port the server is running on.
	 * @param connection_type		The type of connection (e.g. TCP, FTP).
	 * @return true if this replica wasn't already in the data manager, false otherwise.
	 * @param isSM 					True if the database invoking the method is the schema manager.
	 * @throws SQLException 
	 */
	public boolean addReplicaInformation(long modification_id, String databaseLocationOnDisk,
			String string, String hostname, int port, String connectionType,
			int tableSet, boolean isSM) throws RemoteException;

	/**
	 * Removes a particular replica from the schema manager. 
	 * @param dbLocation 
	 * @param machineName 
	 * @param connectionPort 
	 * @param connectionType 
	 * @param schemaName 
	 * @throws SQLException 
	 */
	public int removeReplica(String dbLocation, String machineName, int connectionPort, String connectionType) throws RemoteException, SQLException;

	/**
	 * Remove all stored meta-data for the given data manager.
	 * @throws SQLException 
	 */
	public int removeDataManager() throws RemoteException, SQLException;

	/**
	 * Get the location of a single replica for the given table. This is used in creating linked
	 * tables, so the return type is string rather than DatabaseInstanceRemote.
	 * @return Database connection URL for a given remote database.
	 * @throws RemoteException 
	 */
	public String getLocation() throws RemoteException;

	/**
	 * Release a lock held by the database instance specified in the parameter. Called at the end of QueryProxy.executeQuery()
	 * to indicate that the transaction has finished (it may have succeeded or failed).
	 * @param requestingDatabase	Database which made the original request. Lock was taken out in its name.
	 */
	public void releaseLock(DatabaseInstanceRemote requestingDatabase) throws RemoteException;
}
