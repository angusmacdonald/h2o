package org.h2.h2o.remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

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
	 * The remote interface of the local database instance.
	 */
	private DatabaseInstanceRemote databaseInstance;


	private DatabaseURL localMachineLocation;


	private SchemaManagerReference schemaManagerRef;

	protected static final String LOCAL_DATABASE_INSTANCE = "LOCAL_INSTANCE";

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

		try {
			schemaManagerRef.getSchemaManagerLocationIfNotKnown(chord);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		/*
		 * Create the local database instance remote interface and register it.
		 * 
		 * This must be done before meta-records are executed.
		 */

		this.databaseInstance =  new DatabaseInstance(localMachineLocation, systemSession);

		exportConnectionObject();

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


			if (connected){
				chord.getChordNode().addObserver(chord);

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
		schemaManagerRef.shutdown();
		chord.shutdownNode();
		//		if (this.isSchemaManager){
		//			try {
		//				schemaManager.removeAllTableInformation();
		//			} catch (RemoteException e) {
		//				e.printStackTrace();
		//			}
		//		}
	}

	public void exportConnectionObject() {
		/*
		 * This is done so that the local database instance is exported correctly on RMI. It doesn't seem to
		 * work properly otherwise ('No such object' errors in Database.createH2OTables()). 
		 */
		DatabaseInstanceRemote stub = null;
		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(this.databaseInstance, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		try {
			chord.getLocalRegistry().bind(LOCAL_DATABASE_INSTANCE , stub);

			this.databaseInstance = stub;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#getLocalDatabaseInstance()
	 */
	public DatabaseInstanceRemote getLocalDatabaseInstance() {
		return databaseInstance;
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
	 * @see org.h2.h2o.remote.IDatabaseRemote#getDatabaseInstanceAt(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstanceAt(IChordRemoteReference lookupLocation) throws RemoteException {

		String hostname = lookupLocation.getRemote().getAddress().getHostName();

		int port = lookupLocation.getRemote().getAddress().getPort();

		try {
			return getDatabaseInstanceAt(hostname, port);
		} catch (NotBoundException e) {
			e.printStackTrace();
			return null;
		}
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#getDatabaseInstanceAt(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstanceAt(DatabaseURL dbURL)
			throws RemoteException {
		
		if (dbURL.equals(this.localMachineLocation)){
			return this.getLocalDatabaseInstance(); 
		}

		try {
			return getDatabaseInstanceAt(dbURL.getHostname(), dbURL.getRMIPort());
		} catch (NotBoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Obtain a remote reference to a database instance, where the instance has a chord node running
	 * on the specified hostname and port.
	 * 
	 * <p>This information is used to locate the chord node's RMI registry which provides a reference to
	 * the database instance itself.
	 * @return	Remote reference to the database instance.
	 */
	private DatabaseInstanceRemote getDatabaseInstanceAt(String hostname, int port) throws RemoteException, NotBoundException {
		DatabaseInstanceRemote dir = null;


		Registry remoteRegistry = LocateRegistry.getRegistry(hostname, port);

		dir = (DatabaseInstanceRemote) remoteRegistry.lookup(LOCAL_DATABASE_INSTANCE);

		return dir;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#lookupSchemaManagerNodeLocation()
	 */
	@Override
	public IChordRemoteReference lookupSchemaManagerNodeLocation() throws RemoteException {
		return chord.performChordLookupForSchemaManager();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#getLocalChordReference()
	 */
	@Override
	public IChordRemoteReference getLocalChordReference() {
		return chord.getLocalChordreference();
	}


	public ChordInterface getChordInterface(){
		return chord;
	}


}
