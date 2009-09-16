package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;


/**
 * Remote interface for data manager instances.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DataManagerRemote extends H2ORemote {

	public QueryProxy requestQueryProxy(QueryProxy.LockType lockType) throws RemoteException;

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
}
