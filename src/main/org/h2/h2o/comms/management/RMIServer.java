package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;

import org.h2.h2o.comms.remote.H2ORemote;

import uk.ac.stand.dcs.nds.util.Diagnostic;
import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * Responsible for managing and providing connections to data managers and database instances, both local and remote.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public abstract class RMIServer {

	/**
	 * Default port for RMI registry - used in the case where the schema manager is an in-memory database (where it doesn't
	 * have an exposed TCP port). With TCP databases the RMI registry is exposed on the port above the main TCP server.
	 */
	private static final int DEFAULT_PORT = 20000;

	/**
	 * Proxy for the RMI registry. This is instantiated when on object creation.
	 */
	protected Registry registry;


	/**
	 * Called when the RMI registry is on a remote machine. Registers local data manager interface.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 * @throws RemoteException 
	 */
	public RMIServer(String host, int port) throws RemoteException{
		//If an in-memory database is being run the DB must look for the default port locally.	
		if (port < 1){
			port = RMIServer.DEFAULT_PORT;
			host = null;
		}
		locateRegistry(host, port, false);
	}

	/**
	 * Called by the schema manager to create an RMI registry.
	 * @param port	Port where registry is to be run (on local machine).
	 * @throws RemoteException 
	 */
	public RMIServer(int port) throws RemoteException{
		//If an in-memory database is being run, this must start up on a default port.
		if (port < 1){
			port = RMIServer.DEFAULT_PORT;
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
			Diagnostic.traceNoEvent(Diagnostic.FINAL, "RMI registry is already running.");
		}catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Obtains a reference to the RMI registry.
	 * @param port Port on which the registry is running.
	 * @param host Host on which the registry is running (null indicates it is running locally).
	 * @param startup Whether the registry has been started by this machine. If true, the registry is emptied of old tables.
	 * @throws RemoteException 
	 */
	private void locateRegistry(String host, int port, boolean startup) throws RemoteException{
			if (host == null){
				registry = LocateRegistry.getRegistry(port);
			} else {
				registry = LocateRegistry.getRegistry(host, port);
			}

		if (startup) unbindExistingManagers();

	}
	
	/**
	 * Test that a data manager / database instance can be accessed.
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

		return true;
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
			ErrorHandling.errorNoEvent(e, "Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			ErrorHandling.errorNoEvent(e, "Lost contact with RMI registry when unbinding objects.");
		} catch (NotBoundException e) {
			ErrorHandling.errorNoEvent(e, "Attempting to unbind all objects - failure due to one of the number being unbound.");
		}

	}

	/**
	 * Unbind a given object from the registry.
	 * @param objectName
	 * @param removeLocalOnly 
	 * @throws NotBoundException 
	 */
	public void removeRegistryObject(String objectName, boolean removeLocalOnly) throws NotBoundException {
		if (removeLocalOnly) return;
		
		try {
			
			registry.unbind(objectName);
		
		}  catch (AccessException e) {
			ErrorHandling.errorNoEvent(e, "Didn't have permission to perform unbind operation on RMI registry.");
		} catch (RemoteException e) {
			ErrorHandling.errorNoEvent(e, "Lost contact with RMI registry when unbinding manager of '" + objectName + "'.");
		}
	}

}
