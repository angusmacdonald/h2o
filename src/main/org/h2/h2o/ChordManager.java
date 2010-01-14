package org.h2.h2o;

import java.net.InetSocketAddress;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Observable;
import java.util.Observer;
import java.util.SortedSet;
import java.util.TreeSet;

import org.h2.engine.Constants;

import uk.ac.standrews.cs.nds.eventModel.Event;
import uk.ac.standrews.cs.nds.eventModel.IEvent;
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

	private static IKey schemaManagerKey = new SHA1KeyFactory().generateKey("schemaManager");
	private static IEvent predecessorChangeEvent = new Event("PREDECESSOR_CHANGE_EVENT");

	/**
	 * This set is only maintained if {@link org.h2.engine.Constants#IS_TEST} is true.
	 */
	public static SortedSet<IChordNode> allNodes = new TreeSet<IChordNode>(new NodeComparator());

	/**
	 * Create a new chord ring on the given hostname, port combination.
	 * @param port
	 */
	public void startChordRing(String hostname, int port, String databaseName) {

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
		} catch (P2PNodeException e) {
			e.printStackTrace();
		}

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
		}

		this.currentSMLocation = lookupSchemaManagerLocation();

		((ChordNodeImpl)chordNode).addObserver(this);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + databaseName + " : " + hostname + ":" + port + 
				" : initialized with key :" + chordNode.getKey().toString(10) + " : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : ");

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager key: : : : :" + schemaManagerKey.toString(10) + " : " + schemaManagerKey);

	}

	public void joinChordRing(String localHostname, int localPort, String remoteHostname, int remotePort, String databaseName) {

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
		} catch (P2PNodeException e) {
			e.printStackTrace();
		}	

		if (chordNode == null){
			ErrorHandling.hardError("Failed to create Chord Node.");
		}

		((ChordNodeImpl)chordNode).addObserver(this);


		RingStabilizer.waitForStableNetwork(allNodes);

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Started local Chord node on : " + 
				databaseName + " : " + localHostname + " : " + localPort + " : initialized with key :" + chordNode.getKey().toString(10) + 
				" : " + chordNode.getKey() + " : schema manager at " + currentSMLocation + " : " + chordNode.getSuccessor().getKey());

	}

	public IChordRemoteReference lookupSchemaManagerLocation(){

		try {

			if (chordNode != null){
				IChordRemoteReference newSMLocation = chordNode.lookup(schemaManagerKey);

				return newSMLocation;
			}
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found schema manager at: " + currentSMLocation);

		return null;
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

		if (predecessorChangeEvent.equals(arg)){
			
			if (this.currentSMLocation == null) {
				//This is a new chord node. If it is responsible for the schema manager its successor will say so.
				return;
			} 
			
			IChordRemoteReference newSchemaManagerLocation = lookupSchemaManagerLocation();

			if (!this.currentSMLocation.equals(newSchemaManagerLocation)){
				//We have a new scheam manager location

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
					 * TODO check whether the schema manager ever was in this nodes key space.
					 */


					boolean inKeyRange = chordNode.inLocalKeyRange(schemaManagerKey);

					if (inKeyRange){
						System.err.println("The schema manager is now in the key range of: " + chordNode);
					}
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

	
	
}
