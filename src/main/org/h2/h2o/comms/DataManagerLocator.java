package org.h2.h2o.comms;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.DataManager;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DataManagerLocator extends RMIServer{

	/**
	 * H2O. Data manager instances currently known to the local database instance.
	 */
	private Map<String, DataManagerRemote> dataManagers;

	/**
	 * Called to obtain a connection to the RMI registry.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 */
	public DataManagerLocator(String host, int port) {
		super(host, port);
		
		dataManagers = new HashMap<String, DataManagerRemote>();
	}

	/**
	 * Obtain a proxy for an exposed data manager.
	 * @param tableName	The name of the table whose data manager we are looking for.
	 * @return	Reference to the exposed data manager (under remote interface).
	 */
	public DataManagerRemote lookupDataManager(String tableName) throws SQLException{

		DataManagerRemote dataManager = null;

		dataManager = dataManagers.get(tableName);

		if (dataManager != null){
			return dataManager;
		}
		//The local database doesn't have a reference to the requested DM. Find one via RMI registry.

		try {
			dataManager = (DataManagerRemote) registry.lookup(tableName);
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			System.err.println(tableName + " was not bound to registry.");
			throw new SQLException("Data manager for " + tableName + " could not be found in the registry.");
		}

		return dataManager;
	}
	
	/**
	 * Register the local DM interface with the global RMI registry.
	 * @param interfaceName	Name given to the DM interface on the registry. 
	 * @param dm The data manager instance to be exposed.
	 */
	public void registerDataManager(DataManager dm){

		DataManagerRemote stub = null;

		DataManagerRemote dataManagerRemote = dm;
		try {
			stub = (DataManagerRemote) UnicastRemoteObject.exportObject(dataManagerRemote, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {

			registry.bind(dm.getTableName(), stub);

			dataManagers.put(dm.getTableName(), dm);
		} catch (AlreadyBoundException abe) {

			boolean contactable = testContact(dm.getTableName());

			if (!contactable){
				removeRegistryObject(dm.getTableName());
				registerDataManager(dm);
				System.out.println("An old data manager for " + dm.getTableName() + " was removed.");
			} else {
				System.err.println("A data manager for this table still exists.");
				abe.printStackTrace();
			}
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			System.err.println("Lost contact with RMI registry when attempting to bind a manager.");
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	@Override
	public void removeRegistryObject(String objectName) {
		super.removeRegistryObject(objectName);
		
		dataManagers.remove(objectName);
	}

}
