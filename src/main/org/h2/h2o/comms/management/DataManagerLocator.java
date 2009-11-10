package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.remote.DataManagerRemote;

import uk.ac.stand.dcs.nds.util.Diagnostic;
import uk.ac.stand.dcs.nds.util.ErrorHandling;

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
			Diagnostic.traceNoEvent(Diagnostic.FINAL, tableName + " was not bound to registry.");
			//Happens during the check before CREATE TABLE is run, so is sometimes acceptable.
		} catch (ClassCastException e){
			System.err.println(tableName);
			e.printStackTrace();
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

			registry.rebind(dm.getTableName(), stub);

			dataManagers.put(dm.getTableName(), dm);
		}  catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Lost contact with RMI registry when attempting to bind a manager.");
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	@Override
	public void removeRegistryObject(String objectName, boolean removeLocalOnly) {
		try {
			DataManagerRemote dmr = lookupDataManager(objectName);
			if (dmr != null){
				dmr.removeDataManager();
		
				}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			super.removeRegistryObject(objectName, removeLocalOnly);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		dataManagers.remove(objectName);
	}

}
