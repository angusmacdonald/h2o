package org.h2.h2o.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.h2o.autonomic.Settings;
import org.h2.h2o.comms.DatabaseInstance;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.TableManagerWrapper;
import org.h2.h2o.manager.ISystemTable;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.SystemTableReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.h2.h2o.util.SystemTableReplication;
import org.h2.h2o.util.TableInfo;
import org.h2.h2o.util.locator.DatabaseDescriptorFile;
import org.h2.h2o.util.locator.H2OLocatorInterface;
import org.h2.h2o.util.locator.LocatorClientConnection;
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

	private H2OLocatorInterface locatorServers;

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

		boolean connected = false;

		int attempts = 1; //attempts at connected

		DatabaseURL newSMLocation = null;


		/*
		 * Contact descriptor for SM locations.
		 */
		//		IS:				config\MyFirstDatabase9999.properties
		//		SHOULD BE: 		config\db_data_wrapper__MyFirstDatabase9999.properties
		H2oProperties persistedInstanceInformation = new H2oProperties(localMachineLocation);
		persistedInstanceInformation.loadProperties();
		this.locatorServers = getLocatorServerReference(persistedInstanceInformation);

		/*
		 * Try to connect repeatedly until successful. There is a back-off mechanism to ensure this doesn't fail 
		 * repeatedly in a short space of time.
		 */

		List<String> databaseInstances = null;

		while (!connected && attempts < Settings.ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM){
			try {
				databaseInstances = locatorServers.getLocations();
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


			if (connected){
				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Successfully connected to existing chord ring.");
			} else {
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

					//Obtain a lock on the locator server first.

					boolean locked = false;
					try {
						locked = locatorServers.lockLocators(this.localMachineLocation.getDbLocation());
					} catch (IOException e) {
						throw new StartupException("Couldn't obtain a lock to create a new System Table. " +
								"An IO Exception was thrown trying to contact the locator server (" + e.getMessage() + ").");
					}

					if (locked){
						String chordPort = persistedInstanceInformation.getProperty("chordPort");

						int portToUse = currentPort++;
						if (chordPort!=null){
							Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Obtained chord port from disk: " + chordPort);
							portToUse = Integer.parseInt(chordPort);
						}

						connected = startChordRing(localMachineLocation.getHostname(), portToUse, localMachineLocation);

						if (connected){
							persistedInstanceInformation.setProperty("chordPort", portToUse + "");
							persistedInstanceInformation.saveAndClose();
						}

						newSMLocation = localMachineLocation;
						newSMLocation.setRMIPort(portToUse);

						systemTableRef.setSystemTableURL(newSMLocation);

						//					if (!connected){ //if STILL not connected.
						//						unlockLocator();
						//						throw new StartupException("Tried to connect to an existing database system and couldn't. Also tried to create" +
						//						" a new network and this also failed.");
						//					}
					}
				}


				if (!connected ){
					/*
					 * Back-off then try to connect again up to n times. If this fails, throw an exception.
					 */

					Random r = new Random();
					try {
						int backoffTime = (1000 + (r.nextInt(100) * 10))*attempts;
						Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Trying to connect to Chord ring. Attempt number " + attempts + " of " + Settings.ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM + ". Instance at " + localMachineLocation + " is about to back-off for " + backoffTime + " ms.");

						Thread.sleep(backoffTime);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					attempts++;
				}			
			}
		} 

		/*
		 * If still not connected after many attempts, throw an exception.
		 */
		if (!connected){
			String instances = "";
			for (String instance: databaseInstances){
				instances += instance + "\n";
			}

			throw new StartupException("\n\nAfter " + attempts + " the H2O instance at " + localMachineLocation + " couldn't find an active instance with System Table state, so it cannot connect to the database system.\n\n" +
					"Please re-instantiate one of the following database instances:\n\n" + instances + "\n\n");
		} else {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Database at " + localMachineLocation + " successful created/connected to chord ring.");
		}




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
	 * Get a reference to the locator servers for this database system.
	 * @param persistedInstanceInformation 	Properties file containing the location of the database descriptor.
	 * @return	
	 * @throws StartupException		Thrown if the descriptor file couldn't be found.
	 */
	private H2OLocatorInterface getLocatorServerReference(H2oProperties persistedInstanceInformation) throws StartupException {

		H2OLocatorInterface dlo = null;
		String descriptorLocation = persistedInstanceInformation.getProperty("descriptor");
		String databaseName = persistedInstanceInformation.getProperty("databaseName");

		if (descriptorLocation == null || databaseName == null){
			throw new StartupException("The location of the database descriptor was not specified. The database will now exit.");
		}

		try {
			dlo = new H2OLocatorInterface(databaseName, descriptorLocation);
		} catch (IOException e) {
			throw new StartupException(e.getMessage());
		}
		return dlo;
	}

	/**
	 * Try to join an existing chord ring.
	 * @return True if a connection was successful; otherwise false.
	 */
	private boolean attemptToJoinChordRing(H2oProperties persistedInstanceInformation, DatabaseURL localMachineLocation, List<String> databaseInstances) {


		/*
		 * Try to connect via each of the database instances that are known.
		 */
		for (String url: databaseInstances){
			DatabaseURL instanceURL = DatabaseURL.parseURL(url);

/*
			 * Check first that the location isn't the local database instance (currently running).
			 */
			if (instanceURL.equals(localMachineLocation)) continue;

			//Attempt to connect to a Chord node at this location.
			String chordPort = persistedInstanceInformation.getProperty("chordPort");

			int portToJoinOn = 0;
			if (chordPort!=null){
				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Obtained chord port from disk: " + chordPort);
				portToJoinOn = Integer.parseInt(chordPort);
			} else {
				portToJoinOn = currentPort++;
			}
			
			if (instanceURL.getRMIPort() == portToJoinOn) portToJoinOn++;

			boolean connected = joinChordRing(localMachineLocation.getHostname(), portToJoinOn, instanceURL.getHostname(), instanceURL.getRMIPort(), 
					localMachineLocation.getDbLocationWithoutIllegalCharacters());

			if (connected){
				persistedInstanceInformation.setProperty("chordPort", rmiPort + "");
				persistedInstanceInformation.saveAndClose();
				((ChordNodeImpl)chordNode).addObserver(this);

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL,"Successfully connected to an existing chord ring at " + url);
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

			getLocalRegistry().rebind(LOCAL_DATABASE_INSTANCE , stub);

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
			ErrorHandling.errorNoEvent("Local instance of database " + dbURL + " not bound at " + dbURL.getRMIPort() + "." +
					" Request made by " + localMachineLocation.getURLwithRMIPort());
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

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Trying to connect to existing Chord ring on " + remoteHostname + ":" + remotePort);

		this.rmiPort = localPort;

		InetSocketAddress localChordAddress = new InetSocketAddress(localHostname, rmiPort);
		InetSocketAddress knownHostAddress = new InetSocketAddress(remoteHostname, remotePort);

		boolean connected = false; int attempts = 0;

		while(!connected && attempts < Settings.ATTEMPTS_AFTER_BIND_EXCEPTIONS){ //only have multiple attempts if there is a bind exception. otherwise this just returns false.
			try {
				chordNode = new ChordNodeImpl(localChordAddress, knownHostAddress);
			} catch (ConnectException e){ //database instance we're trying to connect to doesn't exist.
				//e.printStackTrace();
				ErrorHandling.errorNoEvent("Failed to connect to chord node on + " + localHostname + ":" + rmiPort + " known host: " + remoteHostname + ":" + remotePort);
				return false;
			} catch (ExportException e) { //bind exception (most commonly nested in ExportException
				
				if (attempts > 50){
					ErrorHandling.errorNoEvent("Failed to connect to chord ring with known host: " + remoteHostname + ":" + 
							remotePort + ", on address " + localHostname + ":" + rmiPort + ".");
				}
				connected = false;
			} catch (NotBoundException e) {
				ErrorHandling.errorNoEvent("Failed to create new chord node on + " + localHostname + ":" + rmiPort + " known host: " + remoteHostname + ":" + remotePort);
				connected = false;
			} catch (RemoteException e) {
				e.printStackTrace();
				return false;
			} 

			if (chordNode != null){
				connected = true;
			}
			if (!connected) localChordAddress = new InetSocketAddress(localHostname, rmiPort++);
			
			attempts++;
		}

		if (!connected) return false;

		this.systemTableRef.setInKeyRange(false);

		rmiPort = localChordAddress.getPort();
		
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
				databaseName + " : " + localHostname + " : " + rmiPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
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
		//Diagnostic.traceNoEvent(DiagnosticLevel.FULL, arg);

		/*
		 * If the predecessor of this node has changed.
		 */
		if (arg.equals(ChordNodeImpl.PREDECESSOR_CHANGE_EVENT))
			predecessorChangeEvent();
		else if (arg.equals(ChordNodeImpl.SUCCESSOR_CHANGE_EVENT))
			successorChangeEvent();
	}

	/**
	 * Called when the successor has changed. Used to check whether the System Table was on the predecessor, and if it was (and has failed) restart the System Table elsewhere.
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

			/*
			 * There is no guarantee this node has a replica of the System Table state. Obtain the list of replicas from
			 * the locator server. There are a number of cases:
			 * 
			 * 1. This node holds a copy of System Table state. It can then apply to the locator server
			 * 		to become the new System Table.
			 * 2. Another active node holds a copy of the System Table state. This node should be informed of the failure. It can then
			 * 		apply to the locator server itself.
			 * 3. No active node has System Table state. Nothing can be done.
			 */

			if (!systemTableAlive){

				/*
				 * Obtain a reference to the locator servers if one is not already held.
				 */
				if (this.locatorServers == null){
					H2oProperties persistedInstanceInformation = new H2oProperties(localMachineLocation);
					persistedInstanceInformation.loadProperties();
					try {
						this.locatorServers = getLocatorServerReference(persistedInstanceInformation);
					} catch (StartupException e) {
						ErrorHandling.errorNoEvent("Failed to obtain a reference to the locator servers: " + e.getMessage());
						return;
					}
				}

				List<String> stLocations = null;

				try {
					stLocations = locatorServers.getLocations();
				} catch (IOException e) {
					ErrorHandling.errorNoEvent("Failed to obtain a list of instances which hold System Table state: " + e.getMessage());
					return;
				}

				boolean localMachineHoldsSystemTableState = false;
				for (String location: stLocations){
					System.out.println("ST Locations: " + location);
					DatabaseURL st = DatabaseURL.parseURL(location);
					localMachineHoldsSystemTableState = st.equals(localMachineLocation);
				}

				if (localMachineHoldsSystemTableState){
					//Re-instantiate the System Table on this node
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "A copy of the System Table state exists on the successor to the failed machine [" + this.localMachineLocation + "]." +
							" It will be re-instantiated here.");
					systemTableRef.migrateSystemTableToLocalInstance(true, true);
				} else {
					ErrorHandling.hardError("Currently no implemented way of recovering from System Table failure.");
				}
			}
		}

	}

	/**
	 * The successor has changed. Make sure the System Table is replicated to the new successor if this instance is controlling the schema
	 * manager.
	 */
	private void successorChangeEvent() {
		if (Constants.IS_NON_SM_TEST) return; //Don't do this if we're testing something that isn't to do with this replication.

		/*
		 * Check whether there are any table managers running locally.
		 */
		Set<TableManagerWrapper> localTableManagers = null;
		try {
			localTableManagers = this.systemTableRef.getSystemTable().getLocalDatabaseInstances(localMachineLocation);
		} catch (RemoteException e) {
			ErrorHandling.errorNoEvent("Remote exception thrown. Happens when successor has very recently changed and chord ring hasn't stabilized.");
		} catch (MovedException e) {
			try {
				systemTableRef.handleMovedException(e);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}


		IChordRemoteReference successor = chordNode.getSuccessor();
		DatabaseInstanceRemote successorInstance = null;

		/*
		 * If table managers running locally or the System Table is located locally then get a reference to the suceessor instance so
		 * that we can replicate meta-data onto it.
		 * 
		 * If not, don't go to the effort of looking up the successor.
		 */
		if (this.systemTableRef.isSystemTableLocal() || localTableManagers != null && localTableManagers.size() > 0){
			String hostname = null;
			int port = 0;
			try {
				hostname = successor.getRemote().getAddress().getHostName();

				port = successor.getRemote().getAddress().getPort();

				try {
					successorInstance = getDatabaseInstanceAt(hostname, port);
				} catch (NotBoundException e) {
					//May happen. Ignore.
				}


				if (this.systemTableRef.isSystemTableLocal()){
					//The System Table is running locally. Replicate it's state to the new successor.

					try {

						if (successorInstance == null){
							/*
							 * The remote chord node hasn't been fully instantiated yet. Wait a while then try again.
							 */

							if (!Constants.IS_NON_SM_TEST){
								//Don't bother trying to replicate the System Table if this is a test which doesn't require it.
								Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting System Table replication thread on successor change on: " + this.localMachineLocation.getDbLocation() + ".");

								SystemTableReplication newThread = new SystemTableReplication(hostname, port, this.systemTableRef, this);
								newThread.start();
							}

						} else if (successorInstance.equals(this.databaseInstance)) {
							//Do nothing. There is only one node in the network.
							Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "There is only one node in the network so the System Table can't be replicated elsewhere.");
						} else {
							if (successorInstance.isAlive()){
								this.systemTableRef.getSystemTable().addStateReplicaLocation(new DatabaseInstanceWrapper(successorInstance.getConnectionURL(), successorInstance, true));

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

				/*
				 * Now do the same thing for table manager replication.
				 */



				if (localTableManagers != null){
					for (TableManagerWrapper tableManager: localTableManagers){
						//Check that each manager has enough replicas. If not, replicate to the new successor.


					}
				}

			} catch (RemoteException e2) {
				ErrorHandling.exceptionErrorNoEvent(e2, "Couldn't connect to successor.");
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
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Chord node was not initialized so the system is shutting down without transferring any active tables or managers.");
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
					if (dmr.getReplicaManager().getPrimary().getDatabaseInstance().equals(databaseInstance) && dmr.getReplicaManager().getNumberOfReplicas() == 1){
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
				ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
			} catch (MovedException e) {
				ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
			} catch (SQLException e) {
				ErrorHandling.errorNoEvent("(Error during shutdown) " + e.getMessage());
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
			((ChordNodeImpl)chordNode).shutDown();
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

	/**
	 * Called when the local database has been created, has started an ST, and is ready to receive requests.
	 * 
	 * <p>The system will start throwing errors about meta-tables not existing if this is called too soon.
	 */
	public void commitSystemTableCreation() {
		boolean successful = false;

		try {
			successful = locatorServers.commitLocators(this.localMachineLocation.getDbLocation());
		} catch (Exception e) {
			successful = false;
		}

		if (!successful){
			ErrorHandling.errorNoEvent("Failed to unlock database locator servers after creating the system table.");
		}
	}
}
