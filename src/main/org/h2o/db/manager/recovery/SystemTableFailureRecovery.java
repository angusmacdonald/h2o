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

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

import org.h2.engine.Database;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.manager.SystemTable;
import org.h2o.db.manager.SystemTableReference;
import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.remote.IDatabaseRemote;
import org.h2o.db.wrappers.SystemTableWrapper;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class SystemTableFailureRecovery implements ISystemTableFailureRecovery {

	private Database db;
	private SystemTableReference stReference;
	private IDatabaseRemote remoteInterface;

	public SystemTableFailureRecovery(Database db, SystemTableReference systemTableReference) {
		this.db = db;
		this.stReference = systemTableReference;
		this.remoteInterface = db.getRemoteInterface();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2o.db.manager.ISystemTableFailureRecovery#get(org.h2.engine.Database, org.h2o.db.remote.ChordRemote,
	 * org.h2o.db.manager.SystemTableReference)
	 */
	@Override
	public SystemTableWrapper get() throws LocatorException, SystemTableAccessException {
		
		/*
		 * 1. Get the location of the System Table (as the lookup instance currently knows it. 2. Contact the registry of that instance to
		 * get a direct reference to the system table.
		 * 
		 * If this registry (or the System Table) does not exist at this location any more an error will be thrown. This happens when a
		 * query is made before maintenance mechanisms have kicked in.
		 * 
		 * When this happens this node should attempt to find a new location on which to re-instantiate a System Table. This replicates what
		 * is done in ChordRemote.predecessorChangeEvent.
		 */

		try {
			return tryToFindSystemTableViaLocator();
		} catch (SQLException e) {
			ErrorHandling.errorNoEvent(db.getURL() + ": Couldn't find active System Table at any of the locator sites. Will try to recreate System Table elsewhere.");
		}

		SystemTableWrapper systemTableWrapper = reinstantiateSystemTable();

		return systemTableWrapper;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2o.db.manager.ISystemTableFailureRecovery#find(org.h2o.util.exceptions.MovedException, org.h2o.db.remote.ChordRemote)
	 */
	public SystemTableWrapper find(MovedException e) throws SQLException, RemoteException {

		String newLocation = e.getMessage();

		if (newLocation == null) {
			throw new SQLException("The System Table has been shutdown. It must be re-instantiated before another query can be answered.");
		}

		DatabaseURL systemTableLocationURL = DatabaseURL.parseURL(newLocation);
		DatabaseInstanceRemote databaseInstance = lookForDatabaseInstanceAt(remoteInterface, systemTableLocationURL);

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, db.getURL() + ": This System Table reference is old. It has been moved to: " + newLocation);

		if (databaseInstance.isSystemTable()) {
			SystemTableWrapper wrapper = new SystemTableWrapper(databaseInstance.getSystemTable(), databaseInstance.getURL());
			return wrapper;
		} else {
			throw new SQLException(db.getURL() + ": Failed to find new location of System Table at " + systemTableLocationURL);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2o.db.manager.ISystemTableFailureRecovery#restart(org.h2.engine.Database, boolean, boolean,
	 * org.h2o.db.manager.interfaces.SystemTableRemote)
	 */
	public SystemTableWrapper restart(boolean persistedSchemaTablesExist, boolean recreateFromPersistedState,
			SystemTableRemote oldSystemTable) throws SystemTableAccessException {

		if (recreateFromPersistedState) {
			return restartSystemTableFromPersistedState(persistedSchemaTablesExist);

		} else {
			return moveSystemTableToLocalMachine(oldSystemTable);
		}
	}

	/*
	 * 
	 * ################################################
	 * 
	 * Local Recovery Methods...
	 * 
	 * ################################################
	 */

	/**
	 * Recreate the System Table somewhere - the local machine if possible, but if not somewhere else which has persisted copies of the
	 * System Table's state.
	 * 
	 * @param stReference
	 * @param remoteInterface
	 * @param db
	 * @return
	 * @throws LocatorException
	 *             Thrown if the locator server could not be found.
	 * @throws SystemTableAccessException
	 *             Thrown if the System Table could not be restarted.
	 */
	private SystemTableWrapper reinstantiateSystemTable() throws LocatorException, SystemTableAccessException {
		/*
		 * There is no guarantee this node has a replica of the System Table state. Obtain the list of replicas from the locator server.
		 * There are a number of cases:
		 * 
		 * 1. This node holds a copy of System Table state. It can then apply to the locator server to become the new System Table. 2.
		 * Another active node holds a copy of the System Table state. This node should be informed of the failure. It can then apply to the
		 * locator server itself. 3. No active node has System Table state. Nothing can be done.
		 */

		SystemTableWrapper newSystemTableWrapper = null;

		List<String> stLocations = getActiveSystemTableLocationsFromLocator();

		boolean localMachineHoldsSystemTableState = false;
		for (String location : stLocations) {
			DatabaseURL url = DatabaseURL.parseURL(location);
			localMachineHoldsSystemTableState = url.equals(db.getURL());
		}

		if (localMachineHoldsSystemTableState) {
			// Re-instantiate the System Table on this node
			Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getURL() + ": A copy of the System Table state exists on " + db.getURL()
					+ ". It will be re-instantiated here.");
			SystemTableRemote newSystemTable = stReference.migrateSystemTableToLocalInstance(true, true); // throws SystemTableCreationException if it fails.
			newSystemTableWrapper = new SystemTableWrapper(newSystemTable, db.getURL());

		} else {

			Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getURL() + ": Attempting to find another machine which can re-instantiate the System Table.");

			/*
			 * Try to find an active instance with System Table state.
			 */

			newSystemTableWrapper = startSystemTableOnOneOfSpecifiedMachines(stLocations);

		}

		return newSystemTableWrapper;
	}

	/**
	 * Get the list of locations with a current copy of System Table state. The System Table should be running on one of these machines. If
	 * it isn't any one of these machines can be used to re-instantiate it.
	 * 
	 * @param remoteInterface
	 * @return List of DatabaseURLs in String form.
	 * @throws LocatorException
	 */
	private List<String> getActiveSystemTableLocationsFromLocator() throws LocatorException {
		if (remoteInterface == null)
			remoteInterface = db.getRemoteInterface();

		H2OLocatorInterface locatorInterface = remoteInterface.getLocatorInterface();

		if (locatorInterface == null) {
			throw new LocatorException("Failed to find locator servers.");
		}

		List<String> stLocations = null;

		try {
			stLocations = locatorInterface.getLocations();
		} catch (IOException e) {
			throw new LocatorException("Failed to obtain a list of instances which hold System Table state: " + e.getMessage());
		}

		return stLocations;
	}

	/**
	 * The System Table connection has been lost. Try to connect to the System Table lookup location and obtain a reference to the new
	 * System Table.
	 * 
	 * @throws SQLException
	 * @throws LocatorException
	 */
	private SystemTableWrapper tryToFindSystemTableViaLocator() throws SQLException, LocatorException {
		Diagnostic.traceNoEvent(DiagnosticLevel.INIT, db.getURL() + ": Attempting to fix a broken System Table connection.");

		List<String> locatorLocations = getActiveSystemTableLocationsFromLocator();

		DatabaseInstanceRemote databaseInstance = null;

		for (String locatorLocation : locatorLocations) {
			try {
				databaseInstance = lookForDatabaseInstanceAt(remoteInterface, DatabaseURL.parseURL(locatorLocation));

				boolean isSystemTable = databaseInstance.isSystemTable();

				if (isSystemTable) {
					SystemTableWrapper wrapper = new SystemTableWrapper(databaseInstance.getSystemTable(), databaseInstance.getURL());
					return wrapper;
				}
			} catch (Exception e) {
				// May be thrown if database isn't active.
			}
		}

		throw new SQLException(db.getURL() + ": Couldn't find active System Table.");
	}

	/**
	 * Start the System Table on one of the System Table locations given by the stLocations parameter.
	 * 
	 * @param stLocations
	 *            Locations where the new system table could possibly be started.
	 * @return Reference to the new System Table.
	 * @throws SQLException
	 * @throws SystemTableAccessException
	 */
	private SystemTableWrapper startSystemTableOnOneOfSpecifiedMachines(List<String> stLocations) throws SystemTableAccessException {

		for (String systemTableLocation : stLocations) {
			DatabaseURL url = DatabaseURL.parseURL(systemTableLocation);

			DatabaseInstanceRemote databaseInstance = null;

			try {
				databaseInstance = lookForDatabaseInstanceAt(remoteInterface, url);
			} catch (Exception e) {
				// May be thrown if database isn't active.
				continue;
			}

			/*
			 * Attempt to recreate the System Table on this machine.
			 */
			if (databaseInstance != null) {

				SystemTableRemote systemTable = null;
				try {
					systemTable = databaseInstance.recreateSystemTable(); //throws a SystemTableCreationException if it fails.
				} catch (RemoteException e) {
					// May be thrown if database isn't active.
					continue;
				} catch (SQLException e) {
					// Thrown if it failed to create the System Table.
					e.printStackTrace();
					continue; // try another machine.
				}

				return new SystemTableWrapper(systemTable, url);
			}
		}

		throw new SystemTableAccessException("Failed to create new System Table.");
	}

	private DatabaseInstanceRemote lookForDatabaseInstanceAt(IDatabaseRemote iDatabaseRemote, DatabaseURL url) throws RemoteException {
		DatabaseInstanceRemote databaseInstance;
		Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Looking for database instance at: " + url.getHostname() + ":" + url.getRMIPort());
		databaseInstance = iDatabaseRemote.getDatabaseInstanceAt(url);
		return databaseInstance;
	}

	/**
	 * Migrates an active System Table to the local machine.
	 * 
	 * @param database
	 * @param oldSystemTable
	 * @return
	 */
	private SystemTableWrapper moveSystemTableToLocalMachine(SystemTableRemote oldSystemTable) throws SystemTableAccessException {
		/*
		 * CREATE A NEW System Table BY COPYING THE STATE OF THE CURRENT ACTIVE IN-MEMORY System Table.
		 */

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate System Table.");

		SystemTableRemote newSystemTable = null;

		/*
		 * Create a new System Table instance locally.
		 */
		try {
			newSystemTable = new SystemTable(db, true);
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
			throw new SystemTableAccessException("Failed to create new in-memory System Table.");
		}

		/*
		 * Stop the old, remote, manager from accepting any more requests.
		 */
		try {
			oldSystemTable.prepareForMigration(db.getURL().getURLwithRMIPort());
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "This System Table is already being migrated to another instance.");
			throw new SystemTableAccessException("This System Table is already being migrated to another instance.");
		} catch (MovedException e) {
			ErrorHandling.exceptionError(e, "This System Table has already been migrated to another instance.");
			throw new SystemTableAccessException("This System Table is already being migrated to another instance.");
		}

		/*
		 * Build the System Table's state from that of the existing table.
		 */
		try {
			newSystemTable.buildSystemTableState(oldSystemTable);
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to migrate System Table to new machine.");
			throw new SystemTableAccessException("Failed to migrate System Table to new machine.");
		} catch (MovedException e) {
			ErrorHandling
					.exceptionError(e,
							"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			throw new SystemTableAccessException(
					"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
		} catch (SQLException e) {
			ErrorHandling.exceptionError(e, "Couldn't create persisted tables as expected.");
			throw new SystemTableAccessException("Couldn't create persisted tables as expected.");
		} catch (NullPointerException e) {
			// ErrorHandling.exceptionError(e,
			// "Failed to migrate System Table to new machine. Machine has already been shut down.");

		}

		/*
		 * Shut down the old, remote, System Table. Redirect requests to new manager.
		 */
		try {
			oldSystemTable.completeMigration();
		} catch (RemoteException e) {
			ErrorHandling.exceptionError(e, "Failed to complete migration.");
			throw new SystemTableAccessException("Failed to complete migration.");

		} catch (MovedException e) {
			ErrorHandling
					.exceptionError(e,
							"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			throw new SystemTableAccessException(
					"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
		} catch (MigrationException e) {
			ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
			throw new SystemTableAccessException("Migration process timed out. It took too long.");
		}
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "System Table officially migrated to " + db.getURL().getDbLocation() + ".");

		SystemTableWrapper wrapper = new SystemTableWrapper(newSystemTable, db.getURL());
		return wrapper;
	}

	/**
	 * Restarts the System Table on the local machine from persisted state at this machine.
	 * 
	 * @throws SystemTableAccessException
	 */
	private SystemTableWrapper restartSystemTableFromPersistedState(boolean persistedSchemaTablesExist) throws SystemTableAccessException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to re-instantiate System Table from persistent store.");

		SystemTableRemote newSystemTable = null;

		/*
		 * INSTANTIATE A NEW System Table FROM PERSISTED STATE. This must be called if the previous System Table has failed.
		 */
		if (!persistedSchemaTablesExist) {
			ErrorHandling
					.hardError("The system doesn't have a mechanism for recreating the state of the System Table from remote machines.");
		}

		try {
			newSystemTable = new SystemTable(db, false); // false - don't overwrite saved persisted state.
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
			throw new SystemTableAccessException("Failed to create new in-memory System Table.");
		}

		try {
			newSystemTable.buildSystemTableState();

			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, db.getURL() + ": New System Table created.");
		} catch (RemoteException e) {
			e.printStackTrace();
			throw new SystemTableAccessException("Failed to contact some remote process when recreating System Table locally.");
		} catch (MovedException e) {
			e.printStackTrace();
			throw new SystemTableAccessException(
					"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
		} catch (SQLException e) {
			ErrorHandling.exceptionError(e, "Persisted state didn't exist on machine as expected.");
			throw new SystemTableAccessException("Persisted state didn't exist on machine as expected.");
		}

		SystemTableWrapper wrapper = new SystemTableWrapper(newSystemTable, db.getURL());
		
		

		return wrapper;
	}
}
