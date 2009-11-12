package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.AlreadyBoundException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

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
	
	private DatabaseInstanceRemote localInstance;
	
	private DatabaseURL schemaManagerLocation = null; //only known if it belongs to the schema manager instance.


	/**
	 * Called when the RMI registry is on a remote machine. Registers local data manager interface.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 * @throws RemoteException 
	 */
	public DatabaseInstanceLocator(String host, int port) throws RemoteException {
		super(host, port);

		databaseInstances = new HashMap<String, DatabaseInstanceRemote>();
	}


	/**
	 * Called by the schema manager to create an RMI registry.
	 * @param port	Port where registry is to be run (on local machine).
	 * @throws RemoteException 
	 */
	public DatabaseInstanceLocator(int port, DatabaseURL schemaManagerLocation) throws RemoteException {
		super(port);

		this.schemaManagerLocation = schemaManagerLocation; 
		databaseInstances = new HashMap<String, DatabaseInstanceRemote>();
	}


	/**
	 * Connect to an RMI registry running at this location.
	 * @param instanceURL
	 * @throws RemoteException 
	 */
	public DatabaseInstanceLocator(DatabaseURL instanceURL) throws RemoteException {
		this(instanceURL.getHostname(), instanceURL.getPort()+1);
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
		localInstance = databaseInstanceRemote;
		
		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(databaseInstanceRemote, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {

			registry.rebind(databaseInstance.getName(), stub);
			
			/*
			 * Now get this stub back from the registry as a proxy, so it can be added locally. Necessary for later comparisons.
			 * i.e. proxy for object A doesn't equal localy copy of item A in hashtable comparisons. 
			 */
			DatabaseInstanceRemote proxy = (DatabaseInstanceRemote) registry.lookup(databaseInstance.getName());
			databaseInstances.put(databaseInstance.getName(), proxy);
			
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Lost contact with RMI registry when attempting to bind a manager.");
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	@Override
	public synchronized void removeRegistryObject(String objectName, boolean removeLocalOnly) throws NotBoundException {
		super.removeRegistryObject(objectName, removeLocalOnly);

		try {
			if (localInstance.getConnectionString().equals(objectName)){
				UnicastRemoteObject.unexportObject(localInstance, true);
				localInstance = null;
			}
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

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
	public DatabaseInstanceRemote getInstance(String replicaLocationString) {
		try {
			return lookupDatabaseInstance(replicaLocationString);
		} catch (SQLException e) {
			e.printStackTrace();
			ErrorHandling.errorNoEvent("Unable to access database instance at: " + replicaLocationString);
		}
		return null; 
	}


	/**
	 * Get references to all current data managers.
	 * @return
	 */
	public Set<DatabaseInstanceRemote> getInstances() {
		return new HashSet<DatabaseInstanceRemote>(databaseInstances.values());
	}


	/**
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() {
		return schemaManagerLocation;
	}
}
