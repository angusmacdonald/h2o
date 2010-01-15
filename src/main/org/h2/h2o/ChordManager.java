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
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordManager implements Observer {

	private IChordNode chordNode;

	private IChordRemoteReference currentSMLocation;

	private int rmiPort;

	private boolean isSchemaManagerInKeyRange = false;

	private static IKey schemaManagerKey = new SHA1KeyFactory().generateKey("schemaManager");

	/**
	 * This set is only maintained if {@link org.h2.engine.Constants#IS_TEST} is true.
	 */
	public static SortedSet<IChordNode> allNodes = new TreeSet<IChordNode>(new NodeComparator());

	/**
	 * Create a new chord ring on the given hostname, port combination.
	 * @param port
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

		this.currentSMLocation = lookupSchemaManagerLocation();
		this.isSchemaManagerInKeyRange = true;

		((ChordNodeImpl)chordNode).addObserver(this);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + databaseName + " : " + hostname + ":" + port + 
				" : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : ");

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager key: : : : :" + schemaManagerKey.toString(10) + " : " + schemaManagerKey);

		return true;
	}

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

		RingStabilizer.waitForStableNetwork(allNodes);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + 
				databaseName + " : " + localHostname + " : " + localPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
				" : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : " + chordNode.getSuccessor().getKey());


		return true;
	}

	public IChordRemoteReference lookupSchemaManagerLocation(){
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

	/**
	 * @return the chordNode
	 */
	public IChordNode getChordNode() {
		return chordNode;
	}

	/* (non-Javadoc)
	 * @see java.util.Observer#update(java.util.Observable, java.lang.Object)
	 */
	@Override
	public void update(Observable o, Object arg) {

		if (arg.equals(ChordNodeImpl.PREDECESSOR_CHANGE_EVENT)){

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ChordNodeImpl.PREDECESSOR_CHANGE_EVENT);

			if (this.currentSMLocation == null) {
				//This is a new chord node. If it is responsible for the schema manager its successor will say so.
				return;
			} 

			IChordRemoteReference oldSchemaManagerLocation = currentSMLocation;
			
			IChordRemoteReference newSchemaManagerLocation = lookupSchemaManagerLocation();

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

					if (inKeyRange){
						System.err.println("The schema manager is now in the key range of: " + chordNode);
					}

					if (!isSchemaManagerInKeyRange && inKeyRange){ //The schema manager has only just become in the key range of this node.
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager is now in the key range of : " + chordNode);
					} else if (isSchemaManagerInKeyRange && inKeyRange){ //Nothing has changed.
						Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "\tThe schema manager location has not changed.");
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
	 * @return the rmiPort
	 */
	public int getRmiPort() {
		return rmiPort;
	}

	/**
	 * 
	 */
	public void shutdownChordNode() {


	}

	/**
	 * @return
	 */
	public Registry getRegistry() {
		try {
			return LocateRegistry.getRegistry(rmiPort);
		} catch (RemoteException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() {
		IChordRemoteReference sml = lookupSchemaManagerLocation();

		try {
			return getSchemaManagerLocation(sml.getRemote().getAddress().getHostName(), sml.getRemote().getAddress().getPort());
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Occurred when trying to find schema manager.");
			return null;
		}
	}

	/**
	 * @param knownHost
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation(DatabaseURL knownHost) {
		return getSchemaManagerLocation(knownHost.getHostname(), knownHost.getRMIPort());
	}

	private DatabaseURL getSchemaManagerLocation(String hostname, int port) {
		Registry remoteRegistry;
		try {
			remoteRegistry = LocateRegistry.getRegistry(hostname, port);


			DatabaseInstanceRemote dbInstance = (DatabaseInstanceRemote) remoteRegistry.lookup(DatabaseInstanceLocator.LOCAL_DATABASE_INSTANCE);


			return dbInstance.getSchemaManagerLocation();

		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
