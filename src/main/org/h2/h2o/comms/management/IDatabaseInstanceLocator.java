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
package org.h2.h2o.comms.management;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDatabaseInstanceLocator {

	//	/**
	//	 * Obtain a proxy for an exposed Table Manager.
	//	 * @param instanceName	The name of the table whose Table Manager we are looking for.
	//	 * @return	Reference to the exposed Table Manager (under remote interface).
	//	 */
	//	public abstract DatabaseInstanceRemote lookupDatabaseInstance(
	//			String instanceName) throws SQLException;

	/**
	 * @throws NotBoundException 
	 * @throws RemoteException 
	 * @throws RemoteException 
	 * 
	 */
	public void removeLocalInstance() throws NotBoundException, RemoteException;

	public DatabaseInstanceRemote lookupDatabaseInstance(DatabaseURL databaseURL) throws SQLException;

	/**
	 * @return
	 */
	public Set<DatabaseInstanceRemote> getDatabaseInstances();

	//	/**
	//	 * Register the local database instance with the RMI registry.
	//	 * @param databaseInstance Object to be exposed.
	//	 */
	//	public abstract void registerDatabaseInstance(
	//			DatabaseInstance databaseInstance);
	//
	//	/* (non-Javadoc)
	//	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	//	 */
	//	public abstract void removeRegistryObject(String objectName,
	//			boolean removeLocalOnly) throws NotBoundException;
	//
	//	/**
	//	 * @param replicaLocations
	//	 * @return
	//	 */
	//	public abstract Set<DatabaseInstanceRemote> getInstances(
	//			Set<String> replicaLocations);
	//
	//	/**
	//	 * @param replicaLocationString
	//	 * @return
	//	 */
	//	public abstract DatabaseInstanceRemote getInstance(
	//			String replicaLocationString);
	//
	//	/**
	//	 * Get references to all current Table Managers.
	//	 * @return
	//	 */
	//	public abstract Set<DatabaseInstanceRemote> getInstances();
	//
	//	/**
	//	 * @return
	//	 */
	//	public abstract DatabaseURL getSystemTableLocation();

}
