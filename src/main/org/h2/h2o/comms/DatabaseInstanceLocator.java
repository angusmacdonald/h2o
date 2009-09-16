package org.h2.h2o.comms;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.stand.dcs.nds.util.Diagnostic;
import uk.ac.stand.dcs.nds.util.ErrorHandling;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceLocator extends RMIServer {

	/**
	 * Database instances currently known to this database.
	 */
	private Map<String, DatabaseInstanceRemote> databaseInstances;

	/**
	 * Called when the RMI registry is on a remote machine. Registers local data manager interface.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 */
	public DatabaseInstanceLocator(String host, int port) {
		super(host, port);
		
		databaseInstances = new HashMap<String, DatabaseInstanceRemote>();
	}

	
	/**
	 * Called by the schema manager to create an RMI registry.
	 * @param port	Port where registry is to be run (on local machine).
	 */
	public DatabaseInstanceLocator(int port) {
		super(port);
		
		databaseInstances = new HashMap<String, DatabaseInstanceRemote>();
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
			
			e.printStackTrace();
			System.err.println(instanceName + " was not bound to registry.");
			
			String[] inreg;
			try {
				inreg = registry.list();
				for (String in: inreg){
					System.out.print(in + ", ");
				}
			} catch (AccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
			
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
		}

		try {

			registry.bind(databaseInstance.getName(), stub);

			databaseInstances.put(databaseInstance.getName(), databaseInstanceRemote);
		} catch (AlreadyBoundException abe) {

			boolean contactable = testContact(databaseInstance.getName());

			if (!contactable){
				try {
					removeRegistryObject(databaseInstance.getName(), false);
				} catch (NotBoundException e) {
					e.printStackTrace();
				}
				registerDatabaseInstance(databaseInstance);
				Diagnostic.traceNoEvent(Diagnostic.FULL, "An old database instance for " + databaseInstance.getName() + " was removed.");
			} else {
				ErrorHandling.exceptionErrorNoEvent(abe, "A data manager for this table still exists.");
			}
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Lost contact with RMI registry when attempting to bind a manager.");
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	@Override
	public synchronized void removeRegistryObject(String objectName, boolean removeLocalOnly) throws NotBoundException {
		super.removeRegistryObject(objectName, removeLocalOnly);
		
		databaseInstances.remove(objectName);
	}


	/**
	 * @param replicaLocations
	 * @return
	 */
	public Set<DatabaseInstanceRemote> getInstances(Set<String> replicaLocations) {
		Set<DatabaseInstanceRemote> dirs = new HashSet<DatabaseInstanceRemote>();
		
		for (String replicaLocation: replicaLocations){			
			try {
				DatabaseInstanceRemote dir = lookupDatabaseInstance(replicaLocation);
				dirs.add(dir);
			} catch (SQLException e) {
				e.printStackTrace();
				ErrorHandling.errorNoEvent("Unable to access database instance at: " + replicaLocation);
			}
		}
		
		return dirs;
	}


	/**
	 * @param replicaLocationString
	 * @return
	 */
	public DatabaseInstanceRemote getInstances(String replicaLocationString) {
		try {
			DatabaseInstanceRemote dir = lookupDatabaseInstance(replicaLocationString);
			return dir;
		} catch (SQLException e) {
			e.printStackTrace();
			ErrorHandling.errorNoEvent("Unable to access database instance at: " + replicaLocationString);
		}
		return null; //never reached.
	}
}
