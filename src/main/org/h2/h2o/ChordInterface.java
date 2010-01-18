package org.h2.h2o;

import java.net.InetSocketAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Observable;
import java.util.Observer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.h2.engine.Constants;
import org.h2.h2o.comms.management.DatabaseInstanceLocator;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

import uk.ac.standrews.cs.nds.p2p.exceptions.P2PNodeException;
import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.p2p.util.SHA1KeyFactory;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.impl.ChordNodeImpl;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordNode;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;
import uk.ac.standrews.cs.stachordRMI.servers.ChordServer;
import uk.ac.standrews.cs.stachordRMI.util.NodeComparator;
import uk.ac.standrews.cs.stachordRMI.util.RingStabilizer;

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
	 * Reference to the remote chord node which is the location of the current schema manager.
	 */
	private IChordRemoteReference currentSMLocation;

	/**
	 * The port on which the local Chord node is running its RMI server. 
	 */
	private int rmiPort;

	/**
	 * Whether, at the time of last checking, this node is responsible for running the schema manager.
	 */
	private boolean isSchemaManagerInKeyRange = false;

	/**
	 * Key factory used to create keys for schema manager lookup and to search for specific machines.
	 */
	private static SHA1KeyFactory keyFactory;

	/**
	 * The key of the schema manager. This must be used in lookup operations to find the current location of the schema
	 * manager reference.
	 */
	private static IKey schemaManagerKey;

	/**
	 * <p>Set of nodes in the system sorted by key order.
	 * 
	 * <p>This set is only maintained if {@link org.h2.engine.Constants#IS_TEST} is true, and won't
	 * work in anything other than a test environment where each node is in the same address space.
	 */
	public static SortedSet<IChordNode> allNodes;;

	static {
		keyFactory = new SHA1KeyFactory();

		schemaManagerKey = keyFactory.generateKey("schemaManager");

		allNodes = new TreeSet<IChordNode>(new NodeComparator());
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
	public boolean startChordRing(String hostname, int port, String databaseName) {

		this.rmiPort = port;

		InetSocketAddress localChordAddress = new InetSocketAddress(hostname, port);
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Deploying new Chord ring on " + hostname + ":" + port);

		/*
		 * Join the existing Chord Ring.
		 */
		try {
			chordNode  = ChordServer.deployNode(localChordAddress, null);

			if (Constants.IS_TEST){ 
				allNodes.add(chordNode);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		} catch (P2PNodeException e) {
			e.printStackTrace();
			return false;
		}

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
		}

		this.currentSMLocation = lookupSchemaManagerNodeLocation();
		this.isSchemaManagerInKeyRange = true;

		((ChordNodeImpl)chordNode).addObserver(this);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + databaseName + " : " + hostname + ":" + port + 
				" : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : ");
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager key: : : : :" + schemaManagerKey.toString(10) + " : " + schemaManagerKey);

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
			chordNode = ChordServer.deployNode(localChordAddress, knownHostAddress);
			if (Constants.IS_TEST){ 
				allNodes.add(chordNode);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		} catch (P2PNodeException e) {
			e.printStackTrace();
			return false;
		}	

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
			return false;
		}

		((ChordNodeImpl)chordNode).addObserver(this);

		isSchemaManagerInKeyRange = false;

		/*
		 *	Ensure the ring is stable before continuing with any tests. 
		 */
		if (Constants.IS_TEST){ 
			RingStabilizer.waitForStableNetwork(allNodes);
		}
		

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + 
				databaseName + " : " + localHostname + " : " + localPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
				" : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : " + chordNode.getSuccessor().getKey());


		return true;
	}


	/**
	 * Called by various chord functions in {@link ChordNodeImpl} which are being observed. Of particular interest
	 * to this class is the case where the predecessor of a node changes. This is used to assess whether the schema managers
	 * location has changed.
	 * 
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	@Override
	public void update(Observable o, Object arg) {

		/*
		 * If the predecessor of this node has changed.
		 */
		if (arg.equals(ChordNodeImpl.PREDECESSOR_CHANGE_EVENT)){

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ChordNodeImpl.PREDECESSOR_CHANGE_EVENT);

			if (this.currentSMLocation == null) {
				//This is a new chord node. If it is responsible for the schema manager its successor will say so.
				return;
			} 

			IChordRemoteReference oldSchemaManagerLocation = currentSMLocation;

			IChordRemoteReference newSchemaManagerLocation = lookupSchemaManagerNodeLocation();

			if (!oldSchemaManagerLocation.equals(newSchemaManagerLocation)){
				//We have a new schema manager location

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager location has changed.");

				/*
				 * In the case of a new node: this node must check that it still has the schema manager
				 * in its keyspace. If it does nothing else needs to happen. If not, then the new node (the predecessor) is now
				 * in control of the schema manager.
				 * 
				 * In the case of a node failure: this node must check whether it has taken over responsibility for the schema manager.
				 * If it was already responsible for the schema manager then nothing will have changed.
				 */
				try {

					/*
					 * TODO check whether the schema manager was ever in this nodes key space.
					 */

					boolean inKeyRange = chordNode.inLocalKeyRange(schemaManagerKey);

					if (!isSchemaManagerInKeyRange && inKeyRange){ //The schema manager has only just become in the key range of this node.
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager is now in the key range of : " + chordNode);
					} else if (isSchemaManagerInKeyRange && inKeyRange){ //Nothing has changed.
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager location has not changed. It is still in the key range of " + chordNode);
					} else if (isSchemaManagerInKeyRange && !inKeyRange){
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager is no longer in the key range of : " + chordNode);
					}

					isSchemaManagerInKeyRange = inKeyRange;

				} catch (P2PNodeException e) {
					e.printStackTrace();
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
	 */
	public Registry getLocalRegistry() {
		try {
			return LocateRegistry.getRegistry(rmiPort);
		} catch (RemoteException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Returns a reference to the RMI registry of the schema manager.
	 * 
	 * <p>A lookup is performed to identify where the schema manager is currently located,
	 * then the registry is obtained.
	 * 
	 * <p>If the registry is not found this method returns null.
	 * @return	The RMI registry of this chord node.
	 */
	public Registry getSchemaManagerRegistry(){
		Registry remoteRegistry = null;

		if (currentSMLocation == null){
			lookupSchemaManagerNodeLocation();
		}

		try {
			remoteRegistry = LocateRegistry.getRegistry(currentSMLocation.getRemote().getAddress().getHostName(), currentSMLocation.getRemote().getAddress().getPort());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return remoteRegistry;

	}

	/**
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() {
		IChordRemoteReference sml = lookupSchemaManagerNodeLocation();

		try {
			return getSchemaManagerURL(sml.getRemote().getAddress().getHostName(), sml.getRemote().getAddress().getPort());
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Occurred when trying to find schema manager.");
			return null;
		}
	}

	/**
	 * Get the actual location of the schema manager (not the location which a chord lookup
	 * resolves to) from a known host.
	 * @param knownHost	The URL of a known host. This must include the correct hostname and RMI port.
	 * @return A {@link DatabaseURL} object representing the schema managers current location.
	 */
	public DatabaseURL getSchemaManagerURL(DatabaseURL knownHost) {
		return getSchemaManagerURL(knownHost.getHostname(), knownHost.getRMIPort());
	}

	/**
	 * Get the actual location of the schema manager (not the location which a chord lookup
	 * resolves to) from a known host.
	 * @param hostname The hostname of the known host.
	 * @param port		The port on which the known host runs its RMI server.
	 * @return A {@link DatabaseURL} object representing the schema managers current location.
	 */
	private DatabaseURL getSchemaManagerURL(String hostname, int port) {
		try {
			return getRemoteReferenceToDatabaseInstance(hostname, port).getSchemaManagerLocation();
		} catch (RemoteException e) {
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
	private DatabaseInstanceRemote getRemoteReferenceToDatabaseInstance(String hostname, int port) {
		Registry remoteRegistry;
		try {
			remoteRegistry = LocateRegistry.getRegistry(hostname, port);

			DatabaseInstanceRemote dbInstance = (DatabaseInstanceRemote) remoteRegistry.lookup(DatabaseInstanceLocator.LOCAL_DATABASE_INSTANCE);

			return dbInstance;

		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			System.err.println("Hostname : " + hostname + ", Port : " + port);
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Get a reference to the Chord node which is responsible for managing the database's schema manager.
	 * @return	Remote reference to the chord node managing the schema manager.
	 */
	private IChordRemoteReference lookupSchemaManagerNodeLocation(){
		IChordRemoteReference newSMLocation = null;
		
		try {

			if (chordNode != null){
				newSMLocation = chordNode.lookup(schemaManagerKey);
			}

		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found schema manager at: " + newSMLocation);

		currentSMLocation = newSMLocation;

		return newSMLocation;
	}


//	public IChordRemoteReference lookupInstanceLocation(DatabaseURL databaseURL){
//
//		InetSocketAddress address = new InetSocketAddress(databaseURL.getHostname(), databaseURL.getRMIPort());
//
//		IKey node_key = keyFactory.generateKey(address);
//
//		IChordRemoteReference remoteNode = null;
//
//		try {
//			remoteNode = chordNode.lookup(node_key);
//		} catch (RemoteException e) {
//			e.printStackTrace();
//
//		}
//
//		return remoteNode;
//	}
}
