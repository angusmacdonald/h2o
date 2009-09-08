package org.h2.h2o.comms;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.DataManager;

/**
 * Responsible for managing and providing connections to data managers and database instances, both local and remote.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DataManagerLocator {

	/**
	 * Default port for RMI registry - used in the case where the schema manager is an in-memory database (where it doesn't
	 * have an exposed TCP port). With TCP databases the RMI registry is exposed on the port above the main TCP server.
	 */
	private static final int DEFAULT_PORT = 20000;

	/**
	 * Proxy for the RMI registry. This is instantiated when on object creation.
	 */
	private Registry registry;


	/**
	 * H2O. Data manager instances currently known to the local database instance.
	 */
	private Map<String, DataManagerRemote> dataManagers = new HashMap<String, DataManagerRemote>();

	/**
	 * Database instances currently known to this database.
	 */
	private Map<String, DatabaseInstanceRemote> databaseInstances = new HashMap<String, DatabaseInstanceRemote>();

	/**
	 * Called when the RMI registry is on a remote machine. Registers local data manager interface.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 */
	public DataManagerLocator(String host, int port){
		//If an in-memory database is being run the DB must look for the default port locally.	
		if (port < 1){
			port = DataManagerLocator.DEFAULT_PORT;
			host = null;
		}
		locateRegistry(host, port, false);
	}

	/**
	 * Called by the schema manager to create an RMI registry.
	 * @param port	Port where registry is to be run (on local machine).
	 */
	public DataManagerLocator(int port){
		//If an in-memory database is being run, this must start up on a default port.
		if (port < 1){
			port = DataManagerLocator.DEFAULT_PORT;
		}

		initiateRegistry(port);
		locateRegistry(null, port, true);
	}

	/**
	 * Creates an RMI registry locally on the given port.
	 * @param port	Port where the schema manager is running (RMI port to be created is this + 1, or defaults to a 20000 if in-memory).
	 */
	private void initiateRegistry(int port){
		try {
			LocateRegistry.createRegistry(port);

		} catch (ExportException e1) {
			System.err.println("RMI registry is already running.");
		}catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Obtains a reference to the RMI registry.
	 * @param port Port on which the registry is running.
	 * @param host Host on which the registry is running (null indicates it is running locally).
	 * @param startup Whether the registry has been started by this machine. If true, the registry is emptied of old tables.
	 */
	private void locateRegistry(String host, int port, boolean startup){
		try {
			if (host == null){
				registry = LocateRegistry.getRegistry(port);
			} else {
				registry = LocateRegistry.getRegistry(host, port);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		if (startup) unbindExistingManagers();

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
	 * Obtain a proxy for an exposed data manager.
	 * @param instanceName	The name of the table whose data manager we are looking for.
	 * @return	Reference to the exposed data manager (under remote interface).
	 */
	public DatabaseInstanceRemote lookupDatabaseInstance(String instanceName) throws SQLException{

		DatabaseInstanceRemote dbInstance = null;

		dbInstance = databaseInstances.get(instanceName);

		if (dbInstance != null){
			return dbInstance;
		}
		//The local database doesn't have a reference to the requested DM. Find one via RMI registry.

		try {
			dbInstance = (DatabaseInstanceRemote) registry.lookup(instanceName);
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			System.err.println(instanceName + " was not bound to registry.");
			throw new SQLException("Database instance for " + instanceName + " could not be found in the registry.");
		}

		return dbInstance;
	}

	/**
	 * Register the local database instance with the RMI registry.
	 * @param databaseInstance Object to be exposed.
	 */
	public void registerDatabaseInstance(DatabaseInstance databaseInstance) {
		DatabaseInstanceRemote stub = null;

		DatabaseInstanceRemote databaseInstanceRemote = databaseInstance;
		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(databaseInstanceRemote, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (Exception e){
			System.err.println("#############################################");
		}

		try {

			registry.bind(databaseInstance.getName(), stub);

			databaseInstances.put(databaseInstance.getName(), databaseInstanceRemote);
		} catch (AlreadyBoundException abe) {

			boolean contactable = testContact(databaseInstance.getName());

			if (!contactable){
				removeRegistryObject(databaseInstance.getName());
				registerDatabaseInstance(databaseInstance);
				System.out.println("An old database instance for " + databaseInstance.getName() + " was removed.");
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

	/**
	 * Test that a data manager can be accessed.
	 * @param interfaceName	The name given to a data manager in the registry.
	 * @return true if the data manager is accessible; otherwise false.
	 */
	public boolean testContact(String name){

		H2ORemote registryObject;

		try {
			registryObject = (H2ORemote) registry.lookup(name);

			registryObject.testAvailability();

		} catch (Exception e) {
			return false;
		}

		return false;
	}

	/**
	 * Close the RMI registry, removing all exposed data managers.
	 */
	public void unbindExistingManagers() {

		try {

			String[] registryObjects = registry.list();

			for(String objectName: registryObjects){

				registry.unbind(objectName);

			}

			//UnicastRemoteObject.unexportObject(registry,true);

		} catch (AccessException e) {
			System.err.println("Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			System.err.println("Lost contact with RMI registry when unbinding objects.");
		} catch (NotBoundException e) {
			System.err.println("Attempting to unbind all objects - failure due to one of the number being unbound.");
		}

	}

	/**
	 * Unbind a given object from the registry.
	 * @param objectName
	 */
	public void removeRegistryObject(String objectName) {
		try {
			registry.unbind(objectName);

			dataManagers.remove(objectName);
			databaseInstances.remove(objectName);
		}  catch (AccessException e) {
			System.err.println("Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			System.err.println("Lost contact with RMI registry when unbinding manager of '" + objectName + "'.");
		} catch (NotBoundException e) {
			System.err.println("Attempting to unbind manager of '" + objectName + "' - failure due this manager not being bound.");
		}
	}

}
