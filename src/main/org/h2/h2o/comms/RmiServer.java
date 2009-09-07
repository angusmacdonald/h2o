package org.h2.h2o.comms;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;

import org.h2.engine.DataManager;

/**
 * Manages the creation of, and connnections to, the RMI registry storing data manager information.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RmiServer {

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
	 * Called when the RMI registry is on a remote machine. Registers local data manager interface.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 */
	public RmiServer(String host, int port){
		//If an in-memory database is being run the DB must look for the default port locally.	
		if (port < 1){
			port = RmiServer.DEFAULT_PORT;
			host = null;
		}
		locateRegistry(host, port, false);
	}

	/**
	 * Called by the schema manager to create an RMI registry.
	 * @param port	Port where registry is to be run (on local machine).
	 */
	public RmiServer(int port){
		//If an in-memory database is being run, this must start up on a default port.
		if (port < 1){
			port = RmiServer.DEFAULT_PORT;
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
	public IDataManagerRemote lookupDataManager(String tableName){

		IDataManagerRemote dataManager = null;

		try {
			dataManager = (IDataManagerRemote) registry.lookup(tableName);
			System.out.println("Successfully connected to remote DM interface");
		} catch (Exception e) {
			System.err.println("Couldn't connect to remote DM interface");
		}

		return dataManager;
	}

	/**
	 * Register the local DM interface with the global RMI registry.
	 * @param interfaceName	Name given to the DM interface on the registry. 
	 * @param dm The data manager instance to be exposed.
	 */
	public void registerDataManager(DataManager dm){

		IDataManagerRemote stub = null;

		IDataManagerRemote dataManagerRemote = dm;
		try {
			stub = (IDataManagerRemote) UnicastRemoteObject.exportObject(dataManagerRemote, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {
			registry.bind(dm.getTableName(), stub);
		} catch (AlreadyBoundException abe) {
			System.err.println("A data manager with this name is already bound. The system will now exit.");
			abe.printStackTrace();
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Close the RMI registry, removing all exposed data managers.
	 */
	public void unbindExistingManagers() {

		try {

			String[] dataManagers = registry.list();

			for(String dataManager: dataManagers){

				registry.unbind(dataManager);

			}

			//UnicastRemoteObject.unexportObject(registry,true);
			
		} catch (AccessException e) {
			System.err.println("Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			System.err.println("Lost contact with RMI registry when unbinding managers.");
		} catch (NotBoundException e) {
			System.err.println("Attempting to unbind all data managers - failure due to one of the number being unbound.");
		}
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		
		unbindExistingManagers();
	}

	/**
	 * Ubind a given data manager instance from the RMI registry.
	 * @param tableName
	 */
	public void removeDataManager(String tableName) {
		try {
			registry.unbind(tableName);
		}  catch (AccessException e) {
			System.err.println("Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			System.err.println("Lost contact with RMI registry when unbinding manager of '" + tableName + "'.");
		} catch (NotBoundException e) {
			System.err.println("Attempting to unbind manager of '" + tableName + "' - failure due this manager not being bound.");
		}
	}
	
	
}
