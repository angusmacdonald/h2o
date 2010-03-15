package org.h2.h2o.remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.management.DataManagerLocator;
import org.h2.h2o.comms.management.DatabaseInstanceLocator;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.ISchemaManager;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.h2.h2o.util.TableInfo;

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


	private DatabaseURL localMachineLocation;


	private SchemaManagerReference schemaManagerRef;

	/**
	 * Port to be used for the next database instance. Currently used for testing.
	 */
	public static int currentPort = 30000;

	public ChordDatabaseRemote(DatabaseURL localMachineLocation, Database db, SchemaManagerReference schemaManagerRef){
		this.chord = new ChordInterface(db, schemaManagerRef);
		this.schemaManagerRef = schemaManagerRef;

		this.localMachineLocation = localMachineLocation;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#connectToDatabaseSystem(org.h2.h2o.util.DatabaseURL, org.h2.engine.Session)
	 */
	public DatabaseURL connectToDatabaseSystem(Session systemSession) {
		establishChordConnection(localMachineLocation, systemSession);

		this.localMachineLocation.setRMIPort(getLocalRMIPort()); //set the port on which the RMI server is running.

		/*
		 * The schema manager location must be known at this point, otherwise the database instance will not start. 
		 */
		if (schemaManagerRef.getSchemaManagerLocation() != null){
			H2oProperties databaseSettings = new H2oProperties(localMachineLocation);
			databaseSettings.loadProperties();	
			databaseSettings.setProperty("schemaManagerLocation", schemaManagerRef.getSchemaManagerLocation().getUrlMinusSM());
		} else {
			ErrorHandling.hardError("Schema manager not known. This can be fixed by creating a known hosts file (called " + 
					localMachineLocation.getDbLocationWithoutIllegalCharacters() + ".instances.properties) and adding the location of a known host.");
		}

		return schemaManagerRef.getSchemaManagerLocation();
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

			schemaManagerRef.setNewSchemaManagerLocation(newSMLocation);


			if (!connected){ //if STILL not connected.
				ErrorHandling.hardError("Tried to connect to an existing network and couldn't. Also tried to create" +
				" a new network and this also failed.");
			}
		} else {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Successfully connected to existing chord ring.");
		}

		schemaManagerRef.getSchemaManagerLocationIfNotKnown(chord);


		/*
		 * Create the local database instance remote interface and register it.
		 * 
		 * This must be done before meta-records are executed.
		 */

		this.databaseInstance =  new DatabaseInstance(localMachineLocation, systemSession);
		this.databaseInstanceLocator = new DatabaseInstanceLocator(chord, databaseInstance, schemaManagerRef);

		/*
		 * Store another connection to the local RMI registry in order to store data manager references.
		 * 
		 * TODO refactor this out. there are too many references to a single RMI registry.
		 */
		try {
			dataManagerLocator = new DataManagerLocator(chord, schemaManagerRef);
		} catch (RemoteException e) {
			e.printStackTrace();
			ErrorHandling.hardError("This shouldn't happen at this point.");
		}


		if (schemaManagerRef.getSchemaManagerLocation() == null){ // true if the previous check resolved to a node which doesn't know of the schema manager (possibly itself).
			//TODO you probably want a check to make sure it doesn't check against itself.
			System.err.println("should this happen?");
		}

		return schemaManagerRef.getSchemaManagerLocation();
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

			if (!connected){
				portToJoinOn ++;
				connected = chord.joinChordRing(localMachineLocation.getHostname(), portToJoinOn++, instanceURL.getHostname(), instanceURL.getRMIPort(), 
						localMachineLocation.getDbLocationWithoutIllegalCharacters());

			}

			chord.getChordNode().addObserver(chord);

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
		chord.shutdownNode();
		//		if (this.isSchemaManager){
		//			try {
		//				schemaManager.removeAllTableInformation();
		//			} catch (RemoteException e) {
		//				e.printStackTrace();
		//			}
		//		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#registerDataManager(org.h2.h2o.comms.DataManager)
	 */
	public void registerDataManager(DataManager dm) {
		dataManagerLocator.registerDataManager(dm);
		//		try {
		//			schemaManager.addTableInformation(dm, dm.getTableInfo());
		//		} catch (RemoteException e) {
		//			e.printStackTrace();
		//		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#lookupDataManager(java.lang.String)
	 */
	public DataManagerRemote lookupDataManager(String tableName) throws SQLException {
		try {
			ISchemaManager schemaManager = schemaManagerRef.getSchemaManager();

			return schemaManager.lookup(new TableInfo(tableName));
		} catch (RemoteException e) {
			e.printStackTrace();
			return null;
		} catch (MovedException e) {
			schemaManagerRef.handleMovedException(e);
			return lookupDataManager(tableName);
		}
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

		if (databaseInstanceLocator != null) databaseInstanceLocator.removeLocalInstance();

	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.IDatabaseRemote#getLocalMachineLocation()
	 */
	@Override
	public DatabaseURL getLocalMachineLocation() {
		return localMachineLocation;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.IDatabaseRemote#bindSchemaManager(org.h2.h2o.ISchemaManager)
	 */
	@Override
	public void bindSchemaManagerReference(SchemaManagerReference schemaManagerRef) {
		chord.bindSchemaManager(schemaManagerRef);
		//this.schemaManagerRef = schemaManagerRef;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#refindDataManagerReference(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DataManagerRemote refindDataManagerReference(TableInfo ti, DatabaseURL dbURL) {
		DataManagerRemote dataManagerReference = null;
		try {
			Registry remoteRegistry = LocateRegistry.getRegistry(dbURL.getHostname(), dbURL.getRMIPort());
			dataManagerReference = (DataManagerRemote) remoteRegistry.lookup(ti.getFullTableName());
		} catch (Exception e) {
			ErrorHandling.errorNoEvent("Could not find the data manager for " + ti.getFullTableName() + " at its old location: " + dbURL.getHostname() + ":" + dbURL.getRMIPort());
		}
		return dataManagerReference;
	}
}
