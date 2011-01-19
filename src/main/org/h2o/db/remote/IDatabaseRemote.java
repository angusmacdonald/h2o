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
import org.h2o.db.manager.interfaces.ISystemTableReference;
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
     * Returns the port on which the local database instance is running its RMI server.
     * 
     * @return
     */
    public int getRmiPort();

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
     * Export the System Table contained within this SystemTableReference via the UnicastRemoteObject class to allow it to be accessed
     * remotely.
     * 
     * @param systemTableRef
     *            Local wrapper class for the System Table.
     */
    public void exportSystemTable(ISystemTableReference systemTableRef);

    /**
     * Find the database instance located at the location given. The chord reference parameter is used to get the hostname and port of that
     * chord nodes RMI registry. This registry should contain a reference to the local database instance.
     * 
     * @param lookupLocation
     *            The hostname and port of this reference are used to find the local RMI registry.
     * @throws RPCException
     *             Thrown if there is a problem accessing the RMI registry.
     * @return Database instance remote proxy for the database at the given location.
     */
    public IDatabaseInstanceRemote getDatabaseInstanceAt(IChordRemoteReference lookupLocation) throws RPCException, RPCException;

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
}
