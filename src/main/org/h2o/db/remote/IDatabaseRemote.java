/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.remote;

import org.h2.engine.Session;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.manager.recovery.LocatorException;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * The interface between a local database instance and the rest of the database system.
 * 
 * <p>
 * Classes implementing this interface must manage connections to the System Table, and to other database instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDatabaseRemote {

    public DatabaseID connectToDatabaseSystem(Session systemSession, Settings databaseSettings) throws StartupException;

    /**
     * Get the remote reference of the local database instance.
     * 
     * <p>
     * This is used as an identity when locking tables for a particular query.
     * 
     * @return Remote reference of the local database.
     */
    public IDatabaseInstanceRemote getLocalDatabaseInstance();

    /**
     * Remove references to remote objects in preparation for the shutdown of the database system.
     */
    public void shutdown();

    /**
     * True if this chord node is in the process of shutting down.
     */
    public boolean inShutdown();

    /**
     * Get the location of the local database instance, including the port the database is running on (JDBC) and the port the databases RMI
     * connection is running on.
     * 
     * @return Address of the local database instance.
     */
    public DatabaseID getLocalMachineLocation();

    /**
     * Find a database instance located on the machine on which this chord reference is running.
     * 
     * This doesn't guarantee that the same database instance will be returned each time.
     * 
     * @param lookupLocation
     *            The hostname of this reference is used to find a local application registry.
     * @throws RPCException
     *             Thrown if there is a problem accessing the RMI registry.
     * @return Database instance remote proxy for the database at the given location.
     */
    public IDatabaseInstanceRemote getDatabaseInstanceAt(IChordRemoteReference lookupLocation) throws RPCException, RPCException;

    /**
     * Find the database instance located at the location given, with the given database name.
     * 
     * @param hostname
     *            Host on which the RMI registry is located.
     * @param name
     *            ID of the database that this will obtain a reference to.
     * @return Database instance remote proxy for the database at the given location.
     * @throws RPCException
     *             Thrown if there was an error accessing the RMI proxy.
     * @throws NotBoundException
     *             Thrown if there wasn't a database instance interface exposed on the RMI proxy.
     */
    IDatabaseInstanceRemote getDatabaseInstanceAt(String hostname, String name) throws RPCException;

    /**
     * Find the database instance located at the location given. The parameter is used to get the hostname and RMI port of that chord nodes
     * RMI registry. This registry should contain a reference to the local database instance.
     * 
     * @param databaseURL
     *            The hostname and RMI port of this reference are used to find the local RMI registry.
     * @return Database instance remote proxy for the database at the given location.
     */
    public IDatabaseInstanceRemote getDatabaseInstanceAt(DatabaseID databaseURL) throws RPCException;

    public H2OLocatorInterface getLocatorInterface() throws LocatorException;

    /**
     * Get the port on which the local chord node is running.
     * @return
     */
    public int getChordPort();

    /**
     * Produces the key used to store information on the local database instance in the application registry.
     * @return key to query/bind to the application registry.
     */
    public String getApplicationRegistryIDForLocalDatabase();
}
