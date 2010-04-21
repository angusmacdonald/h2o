package org.h2.h2o.remote;

import java.net.InetSocketAddress;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.TableManagerWrapper;
import org.h2.h2o.manager.ISystemTable;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.SystemTableReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.SystemTableReplication;
import org.h2.h2o.util.TableInfo;
import org.h2.h2o.util.properties.DatabaseDescriptorFile;
import org.h2.h2o.util.properties.H2oProperties;
import org.h2.h2o.util.properties.server.LocatorClientConnection;
import org.h2.h2o.util.properties.server.SystemTableLocator;
import org.h2.test.h2o.ChordTests;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.impl.ChordNodeImpl;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordNode;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * <p>Methods defined in IChordInterface relate to the database's interface and interactions with Chord.
 * 
 * <p>Methods defined in IDatabaseRemote represent the rest of the databases distributed state such as
 * remote references to the local databases, database lookup capabilities.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordRemote implements IDatabaseRemote, IChordInterface, Observer {

	/**
	 * The remote interface of the local database instance.
	 */
	private DatabaseInstanceRemote databaseInstance;

	/**
	 * Location information for the local database and chord instance.
	 */
	private DatabaseURL localMachineLocation;

	/**
	 * Local wrapper for the System Table.
	 */
	private ISystemTableReference systemTableRef;

	/**
	 * Name under which the local database instance is bound to its RMI registry.
	 */
	private static final String LOCAL_DATABASE_INSTANCE = "LOCAL_INSTANCE";

	/**
	 * The local chord node for this database instance.
	 */
	private IChordNode chordNode;

	/**
	 * The port on which the local Chord node is running its RMI server. This value should be the same as localMachineLocation.getRMIPort(); 
	 */
	private int rmiPort;

	/**
	 * Used to cache the location of the System Table by asking the known node where it is on startup. This is only
	 * ever really used for this initial lookup. The rest of the System Table funcitonality is hidden behind the 
	 * SystemTableReference object.
	 */
	private DatabaseURL actualSystemTableLocation = null;

	/**
	 * This chord nodes predecessor in the ring. When the predecessor changes this is used to determine if the
	 * System Table was located on the old predecessor, and to check whether it has failed.
	 */
	private IChordRemoteReference predecessor;

	private boolean inShutdown = false;

	/**
	 * Port to be used for the next database instance. Currently used for testing.
	 */
	public static int currentPort = 30000;

	public ChordRemote(DatabaseURL localMachineLocation, ISystemTableReference systemTableRef){
		this.systemTableRef = systemTableRef;
		this.localMachineLocation = localMachineLocation;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.IRemoteDatabase#connectToDatabaseSystem(org.h2.h2o.util.DatabaseURL, org.h2.engine.Session)
	 */
	public DatabaseURL connectToDatabaseSystem(Session session) throws StartupException {
		establishChordConnection(localMachineLocation, session);

		this.localMachineLocation.setRMIPort(getRmiPort()); //set the port on which the RMI server is running.

		/*
		 * The System Table location must be known at this point, otherwise the database instance will not start. 
		 */
		if (systemTableRef.getSystemTableURL() == null){
			ErrorHandling.hardError("System Table not known. This can be fixed by creating a known hosts file (called " + 
					localMachineLocation.getDbLocationWithoutIllegalCharacters() + ".instances.properties) and adding the location of a known host.");
		}

		return systemTableRef.getSystemTableURL();
	}

	/**
	 * Attempt to establish a new Chord connection by trying to connect to a number of known hosts.
	 * 
	 * If no established ring is found a new Chord ring will be created.
	 */
	private DatabaseURL establishChordConnection(DatabaseURL localMachineLocation, Session session) throws StartupException {
		H2oProperties persistedInstanceInformation = new H2oProperties(localMachineLocation);
		persistedInstanceInformation.loadProperties();

		boolean connected = false;
		DatabaseURL newSMLocation = null;


		/*
		 * Contact descriptor for SM locations.
		 */
		String descriptorLocation = persistedInstanceInformation.getProperty("descriptor");
		String databaseName = persistedInstanceInformation.getProperty("databaseName");

		if (descriptorLocation == null || databaseName == null){
			throw new StartupException("The location of the database descriptor was not specified. The database will now exit.");
		}
		SystemTableLocator dl = new SystemTableLocator(databaseName, descriptorLocation);

		Set<String> databaseInstances = null;
		try {
			databaseInstances = dl.getLocations();
		} catch (Exception e){
			e.printStackTrace();
			throw new StartupException(e.getMessage());
		}

		/*
		 * If this is the first time DB to be run the set of DB instance will be empty and this node should become the schema manager.
		 * 
		 * If there is a list of DB instances this instance should attempt to connect to one of them (but not to itself).
		 * 
		 * If none exist but for itself then it can start as the schema manager.
		 * 
		 * If none exist and it isn't on the list either, just shutdown the database.
		 */

		if (databaseInstances != null && databaseInstances.size() > 0){
			/*
			 * There may be a number of database instances already in the ring. Try to connect.
			 */
			connected = attemptToJoinChordRing(persistedInstanceInformation, localMachineLocation, databaseInstances);
		}

		/*
		 * Check whether the local machines URL is included on the list of possible schema managers.
		 */
		boolean localMachineIncluded = false;
		if (databaseInstances != null){
			for (String instance: databaseInstances){
				if (instance.contains(localMachineLocation.getURL())){
					localMachineIncluded = true;
					break;
				}
			}
		}

		if (!connected && (databaseInstances == null || databaseInstances.size() == 0 || localMachineIncluded)){
			/*
			 * Either because there are no known hosts, or because none are still alive.
			 * Create a new chord ring.
			 */

			int portToUse = currentPort++;

			connected = startChordRing(localMachineLocation.getHostname(), portToUse,
					localMachineLocation);


			newSMLocation = localMachineLocation;
			newSMLocation.setRMIPort(portToUse);

			systemTableRef.setSystemTableURL(newSMLocation);


			if (!connected){ //if STILL not connected.
				throw new StartupException("Tried to connect to an existing database system and couldn't. Also tried to create" +
				" a new network and this also failed.");
			}
		}


		if (!connected && (databaseInstances != null && databaseInstances.size() > 0) ){
			
			String instances = "";
			for (String instance: databaseInstances){
				instances += instance + "\n";
			}

			throw new StartupException("\n\nH2O couldn't find an active instance with System Table state, so it cannot connect to the database system.\n\n" +
					"Please re-instantiate one of the following database instances:\n\n" + instances + "\n\n");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Successfully connected to existing chord ring.");


		try {
			DatabaseURL dbURL = systemTableRef.getSystemTableURL();

			if (dbURL == null){
				systemTableRef.setSystemTableURL(getSystemTableLocation());
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		/*
		 * Create the local database instance remote interface and register it.
		 * This must be done before meta-records are executed.
		 */
		this.databaseInstance =  new DatabaseInstance(localMachineLocation, session);

		exportConnectionObject();

		assert systemTableRef.getSystemTableURL() != null;

		return systemTableRef.getSystemTableURL();
	}

	/**
	 * Try to join an existing chord ring.
	 * @return True if a connection was successful; otherwise false.
	 */
	private boolean attemptToJoinChordRing(H2oProperties persistedInstanceInformation, DatabaseURL localMachineLocation, Set<String> databaseInstances) {


		for (String url: databaseInstances){

			DatabaseURL instanceURL = DatabaseURL.parseURL(url);

			/*
			 * Check first that the location isn't the local database instance (currently running).
			 */
			if (instanceURL.equals(localMachineLocation)) continue;

			//Attempt to connect to a Chord node at this location.

			int portToJoinOn = currentPort++;

			if (instanceURL.getRMIPort() == portToJoinOn)
				portToJoinOn++;

			boolean connected = joinChordRing(localMachineLocation.getHostname(), portToJoinOn, instanceURL.getHostname(), instanceURL.getRMIPort(), 
					localMachineLocation.getDbLocationWithoutIllegalCharacters());

			if (!connected){
				portToJoinOn ++;
				connected = joinChordRing(localMachineLocation.getHostname(), portToJoinOn++, instanceURL.getHostname(), instanceURL.getRMIPort(), 
						localMachineLocation.getDbLocationWithoutIllegalCharacters());

			}


			if (connected){
				((ChordNodeImpl)chordNode).addObserver(this);

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL,"Successfully connected to an existing chord ring.");
				return true;
			}

		}

		return false;
	}

	/**
	 * Export the local database instance remote using UnicastRemoteObject.exportObject, so that it is
	 * remotely accessible.
	 */
	private void exportConnectionObject() {
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
			getLocalRegistry().bind(LOCAL_DATABASE_INSTANCE , stub);

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

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#getDatabaseInstanceAt(java.lang.String, int)
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(String hostname, int port) throws RemoteException, NotBoundException {
		DatabaseInstanceRemote dir = null;


		Registry remoteRegistry = LocateRegistry.getRegistry(hostname, port);

		dir = (DatabaseInstanceRemote) remoteRegistry.lookup(LOCAL_DATABASE_INSTANCE);

		return dir;
	}



	/**
	 * Start a new Chord ring at the specified location.
	 * @param hostname	The hostname on which the Chord ring will be started. This must be a local address to the machine
	 * 	on which this process is running.
	 * @param port	The port on which the Chord node will listen. This is port on which the RMI registry will be created.
	 * @param databaseName	The name of the database instance starting this Chord ring. This information is used purely
	 * 	for diagnostic output, so can be left null.
	 * @return	True if the chord ring was started successfully; otherwise false.
	 */
	private boolean startChordRing(String hostname, int port, DatabaseURL databaseURL) {

		this.rmiPort = port;

		InetSocketAddress localChordAddress = new InetSocketAddress(hostname, port);
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Deploying new Chord ring on " + hostname + ":" + port);

		/*
		 * Join the existing Chord Ring.
		 */
		try {
			chordNode  = new ChordNodeImpl(localChordAddress, null);

			if (Constants.IS_TEST){ 
				//allNodes.add(chordNode);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		} catch (NotBoundException e) {
			e.printStackTrace();
			return false;
		}

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
		}

		this.systemTableRef.setLookupLocation(chordNode.getProxy());

		this.actualSystemTableLocation = databaseURL;

		this.systemTableRef.setInKeyRange(true);

		((ChordNodeImpl)chordNode).addObserver(this);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + databaseURL.getDbLocationWithoutIllegalCharacters() + " : " + hostname + ":" + port + 
				" : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : System Table at " + this.systemTableRef.getLookupLocation() + " : ");
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "System Table key: : : : :" + SystemTableReference.systemTableKey.toString(10) + " : " + SystemTableReference.systemTableKey);

		return true;
	}

	/**
	 * Join an existing chord ring.
	 * 	
	 * @param localHostname 	The hostname on which this node will start. This must be a local address to the machine
	 * 	on which this process is running. 
	 * @param localPort			The port on which this node will listen. The RMI server will run on this port.
	 * @param remoteHostname	The hostname of a known host in the existing Chord ring.
	 * @param remotePort		The port on which a known host is listening.
	 * @param databaseName		The name of the database instance starting this Chord ring. This information is used purely
	 * 	for diagnostic output, so can be left null.	
	 * @return true if a node was successfully created and joined an existing Chord ring; otherwise false.
	 */
	private boolean joinChordRing(String localHostname, int localPort, String remoteHostname, int remotePort, String databaseName) {

		this.rmiPort = localPort;

		InetSocketAddress localChordAddress = new InetSocketAddress(localHostname, localPort);
		InetSocketAddress knownHostAddress = new InetSocketAddress(remoteHostname, remotePort);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Connecting to existing Chord ring on " + remoteHostname + ":" + remotePort);

		try {
			chordNode = new ChordNodeImpl(localChordAddress, knownHostAddress);

		} catch (RemoteException e) {
			ErrorHandling.errorNoEvent("Failed to connect to chord ring with known host: " + remoteHostname + ":" + remotePort);
			return false;
		} catch (NotBoundException e) {
			ErrorHandling.errorNoEvent("Failed to create new chord node on + " + localHostname + ":" + localPort + " known host: " + remoteHostname + ":" + remotePort);
			return false;
		}	

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
			return false;
		}

		this.systemTableRef.setInKeyRange(false);


		try {
			DatabaseInstanceRemote lookupInstance = getDatabaseInstanceAt(remoteHostname, remotePort);
			actualSystemTableLocation = lookupInstance.getSystemTableURL();
			this.systemTableRef.setSystemTableURL(actualSystemTableLocation);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + 
				databaseName + " : " + localHostname + " : " + localPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
				" : " + chordNode.getKey() + " : System Table at " + this.systemTableRef.getLookupLocation() + " : " + chordNode.getSuccessor().getKey());


		return true;
	}

	/**
	 * Called by various chord functions in {@link ChordNodeImpl} which are being observed. Of particular interest
	 * to this class is the case where the predecessor of a node changes. This is used to assess whether the System Tables
	 * location has changed.
	 * 
	 * <p>If changing this method please note that it is called synchronously by the Observable class, ChordNodeImpl. This means
	 * that if you try and do something such as chordNode.stabilize() you will possibly introduce some form of deadlock into Chord. This is
	 * difficult to debug, but is the most likely cause of a ring failing to close properly (i.e. not stablizing even after an extended period).
	 * 
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	@Override
	public void update(Observable o, Object arg) {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, arg);

		/*
		 * If the predecessor of this node has changed.
		 */
		if (arg.equals(ChordNodeImpl.PREDECESSOR_CHANGE_EVENT))
			predecessorChangeEvent();
		else if (arg.equals(ChordNodeImpl.SUCCESSOR_CHANGE_EVENT))
			successorChangeEvent();
	}

	/**
	 * Called when the successor has changed. Used to check whether the System Table was on the predecessor, and if it was (and has failed) to restart it
	 * on this machine using local replicated state.
	 */
	private void predecessorChangeEvent() {

		boolean systemTableWasOnPredecessor = systemTableRef.isThisSystemTableNode(this.predecessor);
		this.predecessor = chordNode.getPredecessor();

		if (systemTableWasOnPredecessor){
			//Check whether the System Table is still active.
			boolean systemTableAlive = false;
			try {
				this.systemTableRef.getSystemTable().checkConnection();
				systemTableAlive = true;
			} catch (Exception e) {
				systemTableAlive = false;
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "The System Table is no longer accessible.");
			}


			if (!systemTableAlive){
				// The System Table was on the predecessor and has now failed. It must be re-instantiated on this node.
				systemTableRef.migrateSystemTableToLocalInstance(true, true);
			}
		}

	}

	/**
	 * The successor has changed. Make sure the System Table is replicated to the new successor if this instance is controlling the schema
	 * manager.
	 */
	private void successorChangeEvent() {

		if (this.systemTableRef.isSystemTableLocal()){
			//The System Table is running locally. Replicate it's state to the new successor.
			IChordRemoteReference successor = chordNode.getSuccessor();

			DatabaseInstanceRemote dbInstance = null;

			try {
				String hostname = successor.getRemote().getAddress().getHostName();
				int port = successor.getRemote().getAddress().getPort();

				try {
					dbInstance = getDatabaseInstanceAt(hostname, port);
				} catch (NotBoundException e) {
					//May happen. Ignore.
				}

				if (dbInstance == null){
					/*
					 * The remote chord node hasn't been fully instantiated yet. Wait a while then try again.
					 */

					if (!Constants.IS_NON_SM_TEST){
						//Don't bother trying to replicate the System Table if this is a test which doesn't require it.
						Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting System Table replication thread on successor change on: " + this.localMachineLocation.getDbLocation() + ".");

						SystemTableReplication newThread = new SystemTableReplication(hostname, port, this.systemTableRef, this);
						newThread.start();
					}

				} else if (dbInstance.equals(this.databaseInstance)) {
					//Do nothing. There is only one node in the network.
					Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "There is only one node in the network so the System Table can't be replicated elsewhere.");
				} else {
					if (dbInstance.isAlive()){
						this.systemTableRef.getSystemTable().addStateReplicaLocation(dbInstance);

						//dbInstance.createNewSystemTableBackup(db.getSystemTable());
						//dbInstance.executeUpdate("CREATE REPLICA SCHEMA H2O");

						if (Constants.IS_TEST){
							ChordTests.setReplicated(true);
						}
					}
				}
			} catch (RemoteException e) {
				ErrorHandling.errorNoEvent("Remote exception thrown. Happens when successor has very recently changed and chord ring hasn't stabilized.");
			} catch (MovedException e) {
				try {
					systemTableRef.handleMovedException(e);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}

		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#getRmiPort()
	 */
	public int getRmiPort() {
		return rmiPort;
	}

	/**
	 * Returns a reference to this chord nodes RMI registry.
	 * @return	The RMI registry of this chord node.
	 * @throws RemoteException 
	 */
	private Registry getLocalRegistry() throws RemoteException {
		return LocateRegistry.getRegistry(rmiPort);
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#shutdown()
	 */
	public void shutdown() {

		if (inShutdown){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Chord node is already shutting down: " + chordNode);
			return;
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Shutting down node: " + chordNode);

		inShutdown = true;

		if (chordNode == null){
			ErrorHandling.errorNoEvent("Chord node was not initialized so the system is shutting down without transferring any active tables or managers.");
			return;
		}

		IChordRemoteReference successor = chordNode.getSuccessor();


		boolean successesorIsDifferentMachine = successor != null && !chordNode.getKey().equals(successor.getKey());
		boolean thisIsntATestShouldPreventThis = !Constants.IS_NON_SM_TEST && !Constants.IS_TEAR_DOWN;
		boolean systemTableHeldLocally = systemTableRef.isSystemTableLocal();



		DatabaseInstanceRemote successorDB = null;

		if (successesorIsDifferentMachine && thisIsntATestShouldPreventThis){

			try {
				successorDB = getDatabaseInstanceAt(successor);
			} catch (RemoteException e1) {
				e1.printStackTrace();
			}
		}

		/*
		 * Migrate any local Table Managers.
		 */
		if (successesorIsDifferentMachine && thisIsntATestShouldPreventThis){

			try {
				///successorDB = getDatabaseInstanceAt(successor);


				Set<TableManagerWrapper> localManagers = systemTableRef.getSystemTable().getLocalDatabaseInstances(this.getLocalMachineLocation());


				/*
				 * Create replicas if needed.
				 */
				for (TableManagerWrapper wrapper: localManagers){

					TableManagerRemote dmr = wrapper.getTableManager();
					if (dmr.getReplicaManager().getPrimary().equals(databaseInstance) && dmr.getReplicaManager().getNumberOfReplicas() == 1){
						//This machine holds the only replica - replicate on the successor as well.
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Replicating table [" + wrapper.getTableInfo().getFullTableName() + "] to successor: " + successor);

						successorDB.executeUpdate("CREATE REPLICA " + wrapper.getTableInfo().getFullTableName() + ";", false);
					}
				}


				/*
				 * Migrate Table Managers.
				 */
				for (TableManagerWrapper wrapper: localManagers){

					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Migrating Table Manager [" + wrapper.getTableInfo().getFullTableName() + "] to successor: " + successor);

					successorDB.executeUpdate("MIGRATE TABLEMANAGER " + wrapper.getTableInfo().getFullTableName(), false);

				}

			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Migrate the System Table if needed.
		 */
		if (systemTableHeldLocally && successesorIsDifferentMachine && thisIsntATestShouldPreventThis){

			//Migrate the System Table to this node before shutdown.
			try {
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Migrating System Table to successor: " + successor);
				successorDB = getDatabaseInstanceAt(successor);

				successorDB.executeUpdate("MIGRATE SYSTEMTABLE", false);

			} catch (Exception e) {
				ErrorHandling.errorNoEvent("Failed to migrate System Table to successor: " + successor);
			}
		}




		if (!Constants.IS_NON_SM_TEST){
			((ChordNodeImpl)chordNode).destroy();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#getChordNode()
	 */
	public ChordNodeImpl getChordNode() {
		return (ChordNodeImpl) chordNode;
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#getSystemTableLocation()
	 */
	public DatabaseURL getSystemTableLocation() throws RemoteException{
		if (actualSystemTableLocation != null){ 
			return actualSystemTableLocation; 
		}

		IChordRemoteReference lookupLocation = null;
		lookupLocation = lookupSystemTableNodeLocation();
		systemTableRef.setLookupLocation(lookupLocation);

		String lookupHostname = lookupLocation.getRemote().getAddress().getHostName();
		int lookupPort = lookupLocation.getRemote().getAddress().getPort();

		DatabaseInstanceRemote lookupInstance;
		try {
			lookupInstance = getDatabaseInstanceAt(lookupHostname, lookupPort);

			actualSystemTableLocation = lookupInstance.getSystemTableURL();
			this.systemTableRef.setSystemTableURL(actualSystemTableLocation);
		} catch (NotBoundException e) {
			e.printStackTrace();
		}



		return actualSystemTableLocation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#lookupSystemTableNodeLocation()
	 */
	public IChordRemoteReference lookupSystemTableNodeLocation() throws RemoteException{
		IChordRemoteReference lookupLocation = null;

		if (chordNode != null){
			lookupLocation = chordNode.lookup(SystemTableReference.systemTableKey);
		}

		return lookupLocation;
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#exportSystemTable(org.h2.h2o.manager.SystemTableReference)
	 */
	public void exportSystemTable(ISystemTableReference systemTableRef) {
		ISystemTable stub = null;

		try {
			stub = (ISystemTable) UnicastRemoteObject.exportObject(systemTableRef.getSystemTable(), 0);
			getLocalRegistry().bind(SystemTableReference.SCHEMA_MANAGER, stub);

		} catch (Exception e) {
			e.printStackTrace();
			//ErrorHandling.hardError("Failed to export and bind System Table.");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#getLocalChordReference()
	 */
	public IChordRemoteReference getLocalChordReference() {
		return chordNode.getProxy();
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#getLookupLocation(uk.ac.standrews.cs.nds.p2p.interfaces.IKey)
	 */
	public IChordRemoteReference getLookupLocation(IKey systemTableKey) throws RemoteException {
		return chordNode.lookup(systemTableKey);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IDatabaseRemote#inShutdown()
	 */
	@Override
	public boolean inShutdown() {
		return inShutdown ;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.remote.IChordInterface#bind(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	@Override
	public void bind(String fullTableName, TableManagerRemote stub) {
		try {
			getLocalRegistry().rebind(fullTableName, stub);
		} catch (Exception e) {
			//Doesn't matter.
		}
	}
}
