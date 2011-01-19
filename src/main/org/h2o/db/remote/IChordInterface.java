/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.remote;

import java.rmi.NotBoundException;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordNode;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * Interface to the Chord-specific functionality of the database system.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IChordInterface {

    /**
     * Get the actual location of the System Table by first looking up the location where the 'schemamanager' lookup resoloves to, then
     * querying the database instance at this location for the location of the System Table.
     * 
     */
    DatabaseID getSystemTableLocation() throws RPCException, RPCException;

    /**
     * Get a reference to the Chord node which is responsible for managing the database's System Table lookup, BUT NOT NECESSARILY THE
     * System Table ITSELF.
     * 
     * @return Remote reference to the chord node managing the System Table.
     */
    IChordRemoteReference lookupSystemTableNodeLocation() throws RPCException;

    /**
     * Get the remote chord reference for the local chord node. This can be used for comparison (e.g. to check whether a reference that has
     * been passed in is equal to the local reference) or for lookup operations.
     */
    IChordRemoteReference getLocalChordReference();

    /**
     * Find the database instance located at the location given. The parameters specify the location of the node's RMI registry. This
     * registry should contain a reference to the local database instance.
     * 
     * @param hostname
     *            Host on which the RMI registry is located.
     * @param port
     *            Port on which the RMI registry is located.
     * @return Database instance remote proxy for the database at the given location.
     * @throws RPCException
     *             Thrown if there was an error accessing the RMI proxy.
     * @throws NotBoundException
     *             Thrown if there wasn't a database instance interface exposed on the RMI proxy.
     */
    IDatabaseInstanceRemote getDatabaseInstanceAt(String hostname, int port) throws RPCException, NotBoundException;

    /**
     * Finds the location of the chord node responsible for the given key.
     * 
     * @param key
     *            The key to be used in the lookup.
     * @return The node responsible for the given key.
     */
    IChordRemoteReference getLookupLocation(IKey key) throws RPCException;

    /**
     * Return a reference to the local chord node. This can be used to find the local node's successor or predecessor, or to check whether
     * something is in this nodes key range.
     * 
     * @return the chord node of the local database instance.
     */
    IChordNode getChordNode();

}
