package org.h2.h2o;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.management.DataManagerLocator;
import org.h2.h2o.comms.management.DatabaseInstanceLocator;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordDatabaseRemote implements IDatabaseRemote {

	/**
	 * The databases interface to the Chord ring.
	 */
	private ChordInterface chord;
	
	/**
	 * Interface to other database instances.
	 */
	private DatabaseInstanceLocator databaseInstanceLocator;
	
	/**
	 * Interface to other data managers in the system.
	 */
	private DataManagerLocator dataManagerLocator;

	/**
	 * The remote interface of the local database instance.
	 */
	private DatabaseInstance databaseInstance;
	
	/**
	 * H2O. Indicates whether this database instance is managing the table schema for other running H2O instances.
	 */
	private boolean isSchemaManager;
	
	private DatabaseURL localMachineLocation;

	private DatabaseURL schemaManagerLocation;

	/**
	 * Port to be used for the next database instance. Currently used for testing.
	 */
	public static int currentPort = 30000;

	
	public ChordDatabaseRemote(DatabaseURL localMachineLocation){
		this.chord = new ChordInterface();

		this.localMachineLocation = localMachineLocation;
		
	}
	
	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#connectToDatabaseSystem(org.h2.h2o.util.DatabaseURL, org.h2.engine.Session)
	 */
	public DatabaseURL connectToDatabaseSystem(Session systemSession) {
		schemaManagerLocation = establishChordConnection(localMachineLocation, systemSession);
	
		this.isSchemaManager = localMachineLocation.equals(schemaManagerLocation);
		
		this.localMachineLocation.setRMIPort(getLocalRMIPort()); //set the port on which the RMI server is running.

		/*
		 * The schema manager location must be known at this point, otherwise the database instance will not start. 
		 */
		if (schemaManagerLocation != null){
			H2oProperties databaseSettings = new H2oProperties(localMachineLocation);
			databaseSettings.loadProperties();	
			databaseSettings.setProperty("schemaManagerLocation", getSchemaManagerLocation().getUrlMinusSM());
		} else {
			ErrorHandling.hardError("Schema manager not known. This can be fixed by creating a known hosts file (called " + 
					localMachineLocation.getDbLocationWithoutIllegalCharacters() + ".instances.properties) and adding the location of a known host.");
		}
		
		return schemaManagerLocation;
	}
	
	/**
	 * Get the stored schema manager location (i.e. the system does not have to check whether the schema manager still exists at
	 * this location before returning a value.
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() {
			return chord.getStoredSchemaManagerLocation();
	}

	/**
	 * Attempt to establish a new Chord connection by trying to connect to a number of known hosts.
	 * 
	 * If no established ring is found a new Chord ring will be created.
	 */
	private DatabaseURL establishChordConnection(DatabaseURL localMachineLocation, Session systemSession) {
		H2oProperties persistedInstanceInformation = new H2oProperties(localMachineLocation, "instances");
		boolean knownHostsExist = persistedInstanceInformation.loadProperties();

		boolean connected = false;
		DatabaseURL newSMLocation = null;

		if (knownHostsExist){
			/*
			 * There may be a number of database instances already in the ring. Try to connect.
			 */

			connected = attemptToJoinChordRing(persistedInstanceInformation, localMachineLocation);
		} 

		if (!connected) {
			/*
			 * Either because there are no known hosts, or because none are still alive.
			 * Create a new chord ring.
			 */

			int portToUse = currentPort++;
			
			connected = chord.startChordRing(localMachineLocation.getHostname(), portToUse,
					localMachineLocation);

			newSMLocation = localMachineLocation;
			newSMLocation.setRMIPort(portToUse);
			
			if (!connected){ //if STILL not connected.
				ErrorHandling.hardError("Tried to connect to an existing network and couldn't. Also tried to create" +
				" a new network and this also failed.");
			}
		} else {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Successfully connected to existing chord ring.");
		}

		/*
		 * Create the local database instance remote interface and register it.
		 * 
		 * This must be done before meta-records are executed.
		 */

		this.databaseInstance =  new DatabaseInstance(localMachineLocation, systemSession);
		this.databaseInstanceLocator = new DatabaseInstanceLocator(chord, databaseInstance);

		/*
		 * Store another connection to the local RMI registry in order to store data manager references.
		 * 
		 * TODO refactor this out. there are too many references to a single RMI registry.
		 */
		try {
			dataManagerLocator = new DataManagerLocator(chord);
		} catch (RemoteException e) {
			e.printStackTrace();
			ErrorHandling.hardError("This shouldn't happen at this point.");
		}

		if (newSMLocation == null){ // true if this node has just joined a ring.
			newSMLocation = chord.getActualSchemaManagerLocation();
		}

		if (newSMLocation == null){ // true if the previous check resolved to a node which doesn't know of the schema manager (possibly itself).
			//TODO you probably want a check to make sure it doesn't check against itself.
		}

		return newSMLocation;
	}

	/**
	 * Try to join an existing chord ring.
	 * @return True if a connection was successful; otherwise false.
	 */
	private boolean attemptToJoinChordRing(H2oProperties persistedInstanceInformation, DatabaseURL localMachineLocation) {
		Set<Object> listOfInstances = null;

			listOfInstances = persistedInstanceInformation.getKeys();

			for (Object obj: listOfInstances){
				String url = (String)obj;

				DatabaseURL instanceURL = DatabaseURL.parseURL(url);
				instanceURL.setRMIPort(Integer.parseInt(persistedInstanceInformation.getProperty(url)));

				/*
				 * Check first that the location isn't the local database instance (currently running).
				 */
				if (instanceURL.equals(localMachineLocation)) continue;

				//Attempt to connect to a Chord node at this location.

				
				int portToJoinOn = currentPort++;
				
				if (instanceURL.getRMIPort() == portToJoinOn)
					portToJoinOn++;
				
				boolean connected = chord.joinChordRing(localMachineLocation.getHostname(), portToJoinOn, instanceURL.getHostname(), instanceURL.getRMIPort(), 
						localMachineLocation.getDbLocationWithoutIllegalCharacters());

				if (connected){
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL,"Successfully connected to an existing chord ring.");
					return true;
				}

			}

		return false;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#getLocalRMIPort()
	 */
	public int getLocalRMIPort() {
		return chord.getRmiPort();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#shutdown()
	 */
	public void shutdown() {
		databaseInstanceLocator = null;
		dataManagerLocator = null;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#registerDataManager(org.h2.h2o.comms.DataManager)
	 */
	public void registerDataManager(DataManager dm) {
		dataManagerLocator.registerDataManager(dm);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#lookupDataManager(java.lang.String)
	 */
	public DataManagerRemote lookupDataManager(String tableName) throws SQLException {
		return dataManagerLocator.lookupDataManager(tableName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#removeDataManager(java.lang.String, boolean)
	 */
	public void removeDataManager(String tableName, boolean removeLocalOnly) {
		dataManagerLocator.removeRegistryObject(tableName, removeLocalOnly);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) {
		if (databaseInstanceLocator == null) return null;

		try {
			return databaseInstanceLocator.lookupDatabaseInstance(databaseURL);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#getDatabaseInstances()
	 */
	public Set<DatabaseInstanceRemote> getDatabaseInstances() {
		return databaseInstanceLocator.getDatabaseInstances();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#getLocalDatabaseInstance()
	 */
	public DatabaseInstanceRemote getLocalDatabaseInstance() {
		return databaseInstance;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#removeLocalDatabaseInstance()
	 */
	public void removeLocalDatabaseInstance() throws RemoteException, NotBoundException {
			databaseInstanceLocator.removeLocalInstance();

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IDatabaseRemote#isSchemaManager()
	 */
	@Override
	public boolean isSchemaManager() {
		return isSchemaManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IDatabaseRemote#getLocalMachineLocation()
	 */
	@Override
	public DatabaseURL getLocalMachineLocation() {
		return localMachineLocation;
	}
}
