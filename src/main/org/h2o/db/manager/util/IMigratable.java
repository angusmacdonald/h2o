/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.util;

import org.h2o.db.id.DatabaseID;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.stachord.interfaces.IChordRemoteReference;

/**
 * Classes implementing this interface can be migrated to other machines in the system.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IMigratable {

    /**
     * @throws MovedException
     * 
     */
    public void prepareForMigration(String newLocation) throws RPCException, MigrationException, MovedException;

    /**
     * 
     */
    public void checkConnection() throws RPCException, MovedException;

    /**
     * 
     */
    public void completeMigration() throws RPCException, MovedException, MigrationException;

    /**
     * Tell the manager to stop accepting queries.
     */
    public void shutdown(boolean shutdown) throws RPCException, MovedException;

    /**
     * 
     */
    public IChordRemoteReference getChordReference() throws RPCException;

    /**
     * Get the ID of the database instance that is running this System table using the {@link DatabaseID#getID()} call.
     * @return
     * @throws RPCException
     */
    public DatabaseID getLocalDatabaseID() throws RPCException;
}
