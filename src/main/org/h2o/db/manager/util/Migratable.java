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
package org.h2o.db.manager.util;

import java.rmi.RemoteException;

import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Classes implementing this interface can be migrated to other machines in the
 * system.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface Migratable {
	/**
	 * @throws MovedException
	 * 
	 */
	public void prepareForMigration(String newLocation) throws RemoteException,
			MigrationException, MovedException;

	/**
	 * 
	 */
	public void checkConnection() throws RemoteException, MovedException;

	/**
	 * 
	 */
	public void completeMigration() throws RemoteException, MovedException,
			MigrationException;

	/**
	 * Tell the manager to stop accepting queries.
	 */
	public void shutdown(boolean shutdown) throws RemoteException,
			MovedException;

	/**
	 * 
	 */
	public IChordRemoteReference getChordReference() throws RemoteException;

}
