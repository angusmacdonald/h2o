package org.h2.h2o.comms;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Prepared;

/**
 * Remote interface for data manager instances.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DataManagerRemote extends Remote {

	public void requestLock(String message) throws RemoteException;

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
	 * @throws SQLException 
	 */
	public boolean addReplicaInformation(long modification_id, String databaseLocationOnDisk,
			String string, String hostname, int port, String connectionType,
			int tableSet) throws RemoteException;

	/**
	 * Used to check that a data manager is still accessible via RMI. This method shouldn't do anything -
	 * an exception will be thrown if it is unavailable.
	 */
	public void testAvailability() throws RemoteException;
}
