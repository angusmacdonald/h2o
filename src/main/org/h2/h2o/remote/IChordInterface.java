package org.h2.h2o.remote;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.stachordRMI.impl.ChordNodeImpl;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Interface to the Chord-specific functionality of the database system.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IChordInterface {

	/**
	 * Get the actual location of the schema manager by first looking up the location where the 'schemamanager'
	 * lookup resoloves to, then querying the database instance at this location for the location of the schema manager.
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation() throws RemoteException;

	/**
	 * Get a reference to the Chord node which is responsible for managing the database's schema manager lookup,
	 * BUT NOT NECESSARILY THE SCHEMA MANAGER ITSELF.
	 * @return	Remote reference to the chord node managing the schema manager.
	 * @throws RemoteException 
	 */
	public IChordRemoteReference lookupSchemaManagerNodeLocation() throws RemoteException;

	/**
	 * Get the remote chord reference for the local chord node. This can be used for comparison (e.g. to check whether a reference
	 * that has been passed in is equal to the local reference) or for lookup operations.
	 * @return
	 */
	public IChordRemoteReference getLocalChordReference();

	/**
	 * Find the database instance located at the location given. The parameters specify the location of the node's RMI registry. 
	 * This registry should contain a reference to the local database instance.
	 * @param hostname	Host on which the RMI registry is located.
	 * @param port		Port on which the RMI registry is located.
	 * @return Database instance remote proxy for the database at the given location.
	 * @throws RemoteException		Thrown if there was an error accessing the RMI proxy.
	 * @throws NotBoundException	Thrown if there wasn't a database instance interface exposed on the RMI proxy.
	 */
	public DatabaseInstanceRemote getDatabaseInstanceAt(String hostname, int port) throws RemoteException, NotBoundException;
	
	/**
	 * Finds the location of the chord node responsible for the given key.
	 * @param key		The key to be used in the lookup.
	 * @return	The node responsible for the given key.
	 * @throws RemoteException	
	 */
	public IChordRemoteReference getLookupLocation(IKey key) throws RemoteException;
	
	/**
	 * Return a reference to the local chord node. This can be used to find the local node's successor or predecessor, or to check
	 * whether something is in this nodes key range.
	 * @return	the chord node of the local database instance.
	 */
	public ChordNodeImpl getChordNode();

}
