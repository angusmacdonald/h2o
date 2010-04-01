package org.h2.h2o.remote;

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Observable;
import java.util.Observer;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.ISchemaManager;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.SchemaManagerReplication;
import org.h2.test.h2o.ChordTests;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.impl.ChordNodeImpl;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordNode;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Interface between the database system and Chord, providing various methods which use chord
 * functionality to locate databases and schema managers on the ring.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordInterface implements Observer {

	/**
	 * The node that this ChordManager instance encapsulates.
	 */
	private IChordNode chordNode;



	/**
	 * The port on which the local Chord node is running its RMI server. 
	 */
	private int rmiPort;

	/**
	 * Used to cache the location of the schema manager by asking the known node where it is on startup. This is only
	 * ever really used for this initial lookup. The rest of the schema manager funcitonality is hidden behind the 
	 * SchemaManagerReference object.
	 */
	private DatabaseURL actualSchemaManagerLocation = null;

	private SchemaManagerReference schemaManagerRef;


	private Database db;

	/**
	 * <p>Set of nodes in the system sorted by key order.
	 * 
	 * <p>This set is only maintained if {@link org.h2.engine.Constants#IS_TEST} is true, and won't
	 * work in anything other than a test environment where each node is in the same address space.
	 * @param schemaManagerRef 
	 */
	//public static SortedSet<IChordNode> allNodes = new TreeSet<IChordNode>(new NodeComparator());

	public ChordInterface (Database db, SchemaManagerReference schemaManagerRef){
		this.db = db;
		this.schemaManagerRef = schemaManagerRef;
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
	public boolean startChordRing(String hostname, int port, DatabaseURL databaseURL) {

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

		this.schemaManagerRef.setLookupLocation(chordNode.getProxy());

		this.actualSchemaManagerLocation = databaseURL;

		this.schemaManagerRef.setInKeyRange(true);

		((ChordNodeImpl)chordNode).addObserver(this);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + databaseURL.getDbLocationWithoutIllegalCharacters() + " : " + hostname + ":" + port + 
				" : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : schema manager at " + this.schemaManagerRef.getLookupLocation() + " : ");
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager key: : : : :" + SchemaManagerReference.schemaManagerKey.toString(10) + " : " + SchemaManagerReference.schemaManagerKey);

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
	public boolean joinChordRing(String localHostname, int localPort, String remoteHostname, int remotePort, String databaseName) {

		this.rmiPort = localPort;

		InetSocketAddress localChordAddress = new InetSocketAddress(localHostname, localPort);
		InetSocketAddress knownHostAddress = new InetSocketAddress(remoteHostname, remotePort);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Connecting to existing Chord ring on " + remoteHostname + ":" + remotePort);

		try {
			chordNode = new ChordNodeImpl(localChordAddress, knownHostAddress);
			if (Constants.IS_TEST){ 
				//allNodes.add(chordNode);
			}
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

		//((ChordNodeImpl)chordNode).addObserver(this); //now done by the calling method.

		this.schemaManagerRef.setInKeyRange(false);


		try {
			DatabaseInstanceRemote lookupInstance = getDatabaseInstance(remoteHostname, remotePort);
			actualSchemaManagerLocation = lookupInstance.getSchemaManagerLocation();
			this.schemaManagerRef.setNewSchemaManagerLocation(actualSchemaManagerLocation);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		/*
		 *	Ensure the ring is stable before continuing with any tests. 
		 */

		if (Constants.IS_TEST){


			//RingStabilizer.waitForStableNetwork(allNodes);

			//	for (IChordNode node: allNodes){
			///		System.out.println("CHECK. Suc: " + node.getSuccessor());
			//	}
			//			System.err.println("Schema manager key: " + SchemaManagerReference.schemaManagerKey);
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + 
				databaseName + " : " + localHostname + " : " + localPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
				" : " + chordNode.getKey() + " : schema manager at " + this.schemaManagerRef.getLookupLocation() + " : " + chordNode.getSuccessor().getKey());


		return true;
	}

	/**
	 * Called by various chord functions in {@link ChordNodeImpl} which are being observed. Of particular interest
	 * to this class is the case where the predecessor of a node changes. This is used to assess whether the schema managers
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
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Start of update() : " + arg);

		/*
		 * If the predecessor of this node has changed.
		 */
		if (arg.equals(ChordNodeImpl.PREDECESSOR_CHANGE_EVENT))
			predecessorChangeEvent();
		else if (arg.equals(ChordNodeImpl.SUCCESSOR_CHANGE_EVENT))
			successorChangeEvent();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "End of update() : " + arg);

	}

	/**
	 * 
	 */
	private void predecessorChangeEvent() {
		if (this.schemaManagerRef.getLookupLocation() == null) return; //This is a new chord node. If it is responsible for the schema manager its successor will say so.

	

//		if (schemaManagerWasInPredessorsKeyRange){
			/*
			 * 
			 * At this point in the code we know:
			 * 	- the schema manager (pre-predecessor change) was in the key range of this nodes predecessor.
			 *  - the schema manager itself may have been on this node, but possibly not.
			 * We may have a new schema manager location.
			 * 
			 * 1. The schema manager has failed and this instance is now also responsible for its key space. The local database instance has a replicated
			 * 		copy of its state.
			 * 2. The schema manager lookup is still active on the old predecessor, which is also the chord node with the lookup for 'SCHEMA_MANAGER'.
			 * 			i.e. a new predecessor has joined, but the chord lookup resolves to the schema manager's location.
			 * 3. The schema manager was/is on the predecessor, and this node is not responsible for the lookup 'SCHEMA_MANAGER'.
			 * 			i.e. a new node (the new predecessor) has joined and is now responsible for the lookup operation.
			 */

			/*
//			 * Check whether this is 1.
//			 */
//			boolean scenarioOne = false; //Machine of schema manager lookup has failed.
//			//Check whether the schema manager is still active.
//			try {
//				this.schemaManagerRef.getSchemaManager().checkConnection();
//				scenarioOne = false;
//			} catch (Exception e) {
//				scenarioOne = true;
//				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "The schema manager is no longer accessible.");
//			}
//
//			boolean previousPredecessorAlive = false;
//			//Check whether the node on which the schema manager was on is still active.
//			try {
//				IChordRemoteReference predecessorsSucessor = this.schemaManagerRef.getLookupLocation().getRemote().getSuccessor();
//				if (predecessorsSucessor.equals(this.chordNode.getProxy()))
//					previousPredecessorAlive = true;
//			} catch (RemoteException e) {
//				previousPredecessorAlive = false;
//			}
//
//
//			//We know know if the schema manager is alive, and if the previous predecessor is alive.
//			
//			/*
//			 * Check whether this is 2.
//			 */
//			boolean scenarioTwo = false;
//			/*
//			 * Check whether this is 3.
//			 */
//			boolean scenarioThree = false;
//
//
//			/*
//			 * Now we know what has happened. Action...
//			 */
//			if (scenarioOne){ //1. The schema manager has failed and this instance is now responsible for its key space.
//				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager is now in the key range of : " + chordNode);
//
//				schemaManagerRef.migrateSchemaManagerToLocalInstance(true, true);
//				this.schemaManagerRef.setInKeyRange(true);
//
//			} else if (scenarioTwo){ // 2. The schema manager is still active on the old predecessor, which is also the chord node with the lookup for 'SCHEMA_MANAGER'.
//				schemaManagerRef.setInPredessorsKeyRange(false); // No longer true.
//				//Nothing else needs changing on this machine. The new predecessor will have a pointer to the actual schema manager.
//			} else if (scenarioThree){ // 3. The schema manager was/is on the predecessor, and this node is not responsible for the lookup 'SCHEMA_MANAGER'.
//
//			}

//		}
	}

	/**
	 * The successor has changed. Make sure the schema manager is replicated to the new successor if this instance is controlling the schema
	 * manager.
	 */
	private void successorChangeEvent() {

		if (this.schemaManagerRef.isSchemaManagerLocal()){
			//The schema manager is running locally. Replicate it's state to the new successor.
			IChordRemoteReference successor = chordNode.getSuccessor();

			DatabaseInstanceRemote dbInstance = null;

			try {
				String hostname = successor.getRemote().getAddress().getHostName();
				int port = successor.getRemote().getAddress().getPort();

				dbInstance = getDatabaseInstance(hostname, port);

				if (dbInstance == null){
					/*
					 * The remote chord node hasn't been fully instantiated yet. Wait a while then try again.
					 */

					if (!Constants.IS_NON_SM_TEST){
						//Don't bother trying to replicate the schema manager if this is a test which doesn't require it.
						SchemaManagerReplication newThread = new SchemaManagerReplication(hostname, port, this.db, this);
						newThread.start();
					}

				} else {

					this.schemaManagerRef.getSchemaManager(false).addSchemaManagerDataLocation(dbInstance);

					//dbInstance.createNewSchemaManagerBackup(db.getSchemaManager());
					//dbInstance.executeUpdate("CREATE REPLICA SCHEMA H2O");

					if (Constants.IS_TEST){
						ChordTests.setReplicated(true);
					}

				}
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				try {
					schemaManagerRef.handleMovedException(e);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}



		}
	}

	/**
	 * Returns the port on which this chord nodes RMI server is running.
	 * @return RMI registry port for this chord node.
	 */
	public int getRmiPort() {
		return rmiPort;
	}

	/**
	 * Returns a reference to this chord nodes RMI registry.
	 * @return	The RMI registry of this chord node.
	 * @throws RemoteException 
	 */
	public Registry getLocalRegistry() throws RemoteException {
		return LocateRegistry.getRegistry(rmiPort);
	}

	/**
	 * 
	 */
	public void shutdownNode() {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Shutting down node: " + chordNode);

		if (!Constants.IS_NON_SM_TEST){
			//allNodes.remove(chordNode);
			((ChordNodeImpl)chordNode).destroy();
		}
	}

	/**
	 * @return
	 */
	public ChordNodeImpl getChordNode() {
		return (ChordNodeImpl) chordNode;
	}

	/**
	 * Get the actual location of the schema manager by first looking up the location where the 'schemamanager'
	 * lookup resoloves to, then querying the database instance at this location for the location of the schema manager.
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() throws RemoteException{
		if (actualSchemaManagerLocation != null){ 
			return actualSchemaManagerLocation; 
		}

		IChordRemoteReference lookupLocation = null;
		lookupLocation = performChordLookupForSchemaManager();
		schemaManagerRef.setLookupLocation(lookupLocation);

		String lookupHostname = lookupLocation.getRemote().getAddress().getHostName();
		int lookupPort = lookupLocation.getRemote().getAddress().getPort();

		DatabaseInstanceRemote lookupInstance = getDatabaseInstance(lookupHostname, lookupPort);

		actualSchemaManagerLocation = lookupInstance.getSchemaManagerLocation();
		this.schemaManagerRef.setNewSchemaManagerLocation(actualSchemaManagerLocation);

		return actualSchemaManagerLocation;
	}

	/**
	 * Obtain a remote reference to a database instance, where the instance has a chord node running
	 * on the specified hostname and port.
	 * 
	 * <p>This information is used to locate the chord node's RMI registry which provides a reference to
	 * the database instance itself.
	 * @return	Remote reference to the database instance.
	 */
	public DatabaseInstanceRemote getDatabaseInstance(String hostname, int port) {
		Registry remoteRegistry;
		try {
			remoteRegistry = LocateRegistry.getRegistry(hostname, port);

			DatabaseInstanceRemote dbInstance = (DatabaseInstanceRemote) remoteRegistry.lookup(ChordDatabaseRemote.LOCAL_DATABASE_INSTANCE);

			return dbInstance;

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Database Instance wasn't bound on hostname : " + hostname + ", Port : " + port);
		}
		return null;
	}

	/**
	 * Get a reference to the Chord node which is responsible for managing the database's schema manager lookup,
	 * BUT NOT NECESSARILY THE SCHEMA MANAGER ITSELF.
	 * @return	Remote reference to the chord node managing the schema manager.
	 * @throws RemoteException 
	 */
	public IChordRemoteReference performChordLookupForSchemaManager() throws RemoteException{
		IChordRemoteReference lookupLocation = null;

		if (chordNode != null){
			lookupLocation = chordNode.lookup(SchemaManagerReference.schemaManagerKey);
		}

		return lookupLocation;
	}

	/**
	 * Export the local schema manager object and bind it to the local RMI registry.
	 * @param schemaManagerRef Contains a reference to the schema manager object being bound.
	 */
	public void bindSchemaManager(SchemaManagerReference schemaManagerRef) {
		ISchemaManager stub = null;

		try {
			stub = (ISchemaManager) UnicastRemoteObject.exportObject(schemaManagerRef.getSchemaManager(), 0);
			getLocalRegistry().bind(SchemaManagerReference.SCHEMA_MANAGER, stub);

		} catch (Exception e) {
			e.printStackTrace();
			//ErrorHandling.hardError("Failed to export and bind schema manager.");
		}
	}
}
