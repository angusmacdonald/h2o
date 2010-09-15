/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.recovery;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.wrappers.SystemTableWrapper;
import org.h2o.util.exceptions.MovedException;

/**
 * Provides utilities to find and recover the System Table in case of failure.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface ISystemTableFailureRecovery {


	/**
	 * Responsible for finding the System Table if the current reference is invalid. If no active System Table is found
	 * this will create a System Table somewhere valid.
	 * 
	 * @param stReference
	 *            Responsible for caching reference to the System Table.
	 * @param remoteInterface
	 *            Responsible for communicating with remote instances and locator servers.
	 * @return A current reference to an active System Table.
	 * @throws LocatorException		Thrown if the locator server(s) could not be found.
	 * @throws SystemTableAccessException			Thrown if the System Table could not be recreated.
	 */
	public SystemTableWrapper get()
			throws LocatorException, SystemTableAccessException;

	/**
	 * Restart the System Table on the local machine.
	 * 
	 * @param database
	 *            Reference to the local database object.
	 * @param persistedSchemaTablesExist
	 *            Whether this machine has persisted copies of System Table state.
	 * @param recreateFromPersistedState
	 *            If true the database will be recreated from persisted state. If false it will be copied from an existing active System
	 *            Table.
	 * @param oldSystemTable
	 *            The reference that this database has for the old System Table. This will be used if creating from an active copy.
	 * @return Reference to the new System Table.
	 * @throws SystemTableAccessException	Thrown when the restart of the System Table failed.
	 */
	public SystemTableWrapper restart(boolean persistedSchemaTablesExist,
			boolean recreateFromPersistedState, SystemTableRemote oldSystemTable) throws SystemTableAccessException ;

	
	/**
	 * Find the System Table after a MovedException is thrown on SystemTable lookup.
	 * 
	 * An exception has been thrown trying to access the System Table because it has been moved to a new location. This method handles this
	 * by updating the reference to that of the new System Table.
	 * 
	 * @param e
	 *            The MovedException that was thrown.
	 * @return Reference to the new System Table.
	 * @throws SQLException
	 *             Thrown if the System Table was not found at the specified new location.
	 * @throws RemoteException
	 *             Thrown if it wasn't possible to connect to the database at the new specified location.
	 */
	public SystemTableWrapper find(MovedException e) throws SQLException, RemoteException;
}
