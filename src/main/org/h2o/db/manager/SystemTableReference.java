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
package org.h2o.db.manager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.table.ReplicaSet;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.manager.interfaces.SystemTableRemote;
import org.h2o.db.remote.IChordInterface;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.event.DatabaseStates;
import org.h2o.event.client.H2OEvent;
import org.h2o.event.client.H2OEventBus;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.p2p.util.SHA1KeyFactory;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Encapsulates SystemTable references, containing state on whether the reference is local or remote, whether the lookup is local or remote,
 * and other relevant information. This class manages operations on the System Table, such as migration between database instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableReference implements ISystemTableReference {

	/**
	 * Name under which the System Table is located in the local RMI registry.
	 */
	public static final String SCHEMA_MANAGER = "SCHEMA_MANAGER";

	/*
	 * System Table STATE.
	 */
	/**
	 * Reference to the database system's System Table.
	 */
	private SystemTableRemote systemTable;

	private Map<TableInfo, TableManagerRemote> cachedTableManagerReferences = new HashMap<TableInfo, TableManagerRemote>();
	private Map<TableInfo, TableManager> localTableManagers = new HashMap<TableInfo, TableManager>();

	/**
	 * Location of the actual System Table.
	 */
	private DatabaseURL systemTableLocationURL = null;

	/**
	 * Whether the System Table is running on this node.
	 */
	private boolean isLocal = false;

	/**
	 * Whether the System Table lookup on Chord resolves to this machines keyspace.
	 */
	private boolean inKeyRange = false;

	private IChordRemoteReference systemTableNode;

	/*
	 * CHORD-RELATED.
	 */
	/**
	 * Reference to the remote chord node which is responsible for ensuring the System Table is running. This node is not necessarily the
	 * actual location of the System Table.
	 */
	private IChordRemoteReference lookupLocation;

	/**
	 * Key factory used to create keys for System Table lookup and to search for specific machines.
	 */
	private static SHA1KeyFactory keyFactory = new SHA1KeyFactory();

	/**
	 * The key of the System Table. This must be used in lookup operations to find the current location of the schema manager reference.
	 */
	public static IKey systemTableKey = keyFactory.generateKey("systemTable");

	/*
	 * GENERAL DATABASE.
	 */
	/**
	 * Reference to the local database instance. This is needed to get the local database URL, and to instantiate new System Table objects.
	 */
	private Database db;

	/**
	 * When a new object is created with this constructor the System Table reference may not exist, so only the database object is required.
	 * 
	 * @param db
	 */
	public SystemTableReference(Database db) {
		this.db = db;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTable()
	 */
	public SystemTableRemote getSystemTable() {
		return getSystemTable(false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTable(boolean)
	 */
	public SystemTableRemote getSystemTable(boolean inShutdown) {
		return getSystemTable(false, inShutdown);
	}

	private SystemTableRemote getSystemTable(boolean performedSystemTableLookup, boolean inShutdown) {
		try {

			if (systemTable == null) {
				performedSystemTableLookup = true;
				systemTable = this.findSystemTable();
			}

			systemTable.checkConnection();
		} catch (MovedException e) {
			if (!inShutdown) {
				try {
					this.handleMovedException(e);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		} catch (SQLException e) {
			/*
			 * Called when findSystemTable() has failed to find the System Table instances registry.
			 * This indicates that the system table instance has failed, so we should try to recreate the
			 * System Table somewhere else.
			 */
//			try {
				ErrorHandling.errorNoEvent(this.db.getURL() + ": Failed to find System Table. Attempting to re-instantiate it on a valid instance (via locator servers).");
//				recreateSchemaManager();
//			} catch (RemoteException e1) {
//				e1.printStackTrace();
//			}

			return systemTable;
		} catch (Exception e) {
			ErrorHandling.errorNoEvent("Failed to find System Table: " + e.getMessage());
			/*
			 * Call this method again if we attempted to access a cached System Table reference and it didn't work.
			 */
			if (!performedSystemTableLookup) {
				systemTable = null;
				return getSystemTable(true, inShutdown);
			}

			ErrorHandling.errorNoEvent("System Table is not accessible");
		}

		if (systemTable != null){
			try {
				this.systemTableNode = systemTable.getChordReference();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		return systemTable;
	}

	/**
	 * Recreates the System Table somewhere using the locator server(s).
	 * @throws RemoteException 
	 */
	private void recreateSchemaManager() throws RemoteException {
		systemTable = this.db.getRemoteInterface().reinstantiateSystemTable();

		this.systemTableNode = systemTable.getChordReference();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableLocation()
	 */
	public DatabaseURL getSystemTableURL() {
		return systemTableLocationURL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#isSystemTableLocal()
	 */
	public boolean isSystemTableLocal() {
		return isLocal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#findSystemTable()
	 */
	public SystemTableRemote findSystemTable() throws SQLException {
		if (systemTable != null) {
			return systemTable;
		}

		Registry registry = getSystemTableRegistry();

		try {
			systemTable = (SystemTableRemote) registry.lookup(SCHEMA_MANAGER);
			this.systemTableNode = systemTable.getChordReference();
		} catch (Exception e) {
			throw new SQLException("Unable to find System Table. Attempted to find it through the registry at " + systemTableLocationURL);
		}

		return systemTable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableRegistry()
	 */
	public Registry getSystemTableRegistry() {
		Registry remoteRegistry = null;

		try {
			remoteRegistry = LocateRegistry.getRegistry(systemTableLocationURL.getHostname(), systemTableLocationURL.getRMIPort());

		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return remoteRegistry;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setNewSystemTableLocation(org .h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableURL(DatabaseURL newSMLocation) {
		if (newSMLocation.equals(db.getURL())) {
			isLocal = true;
		}

		this.systemTableLocationURL = newSMLocation;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTableLocation(uk.ac
	 * .standrews.cs.stachordRMI.interfaces.IChordRemoteReference, org.h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableLocation(IChordRemoteReference systemTableLocation, DatabaseURL databaseURL) {
		this.systemTableNode = systemTableLocation;
		this.systemTableLocationURL = databaseURL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTable(org.h2.h2o.manager .SystemTable)
	 */
	public void setSystemTable(SystemTableRemote systemTable) {
		this.systemTable = systemTable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTableLocationURL(org .h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableLocationURL(DatabaseURL databaseURL) {
		this.systemTableLocationURL = databaseURL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#isConnectedToSM()
	 */
	public boolean isConnectedToSM() {
		return (systemTable != null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setInKeyRange(boolean)
	 */
	public void setInKeyRange(boolean inKeyRange) {
		this.inKeyRange = inKeyRange;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#isInKeyRange()
	 */
	public boolean isInKeyRange() {
		return inKeyRange;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance (boolean, boolean)
	 */
	public SystemTableRemote migrateSystemTableToLocalInstance(boolean persistedSchemaTablesExist, boolean recreateFromPersistedState) {

		if (recreateFromPersistedState) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to re-instantiate System Table from persistent store.");
			/*
			 * INSTANTIATE A NEW System Table FROM PERSISTED STATE. This must be called if the previous System Table has failed.
			 */
			if (!persistedSchemaTablesExist) {
				ErrorHandling.hardError("The system doesn't have a mechanism for recreating the state of the System Table from remote machines.");
			}

			SystemTableRemote newSystemTable = null;
			try {
				newSystemTable = new SystemTable(db, false); // false - don't overwrite saved persisted state.
			} catch (Exception e) {
				ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
			}

			try {
				newSystemTable.buildSystemTableState();

				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, db.getURL() + ": New System Table created.");
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				ErrorHandling.exceptionError(e, "Persisted state didn't exist on machine as expected.");
			}

			systemTable = newSystemTable;

		} else {
			/*
			 * CREATE A NEW System Table BY COPYING THE STATE OF THE CURRENT ACTIVE IN-MEMORY System Table.
			 */
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate System Table.");

			/*
			 * Create a new System Table instance locally.
			 */
			SystemTableRemote newSystemTable = null;
			try {
				newSystemTable = new SystemTable(db, true);
			} catch (Exception e) {
				ErrorHandling.exceptionError(e, "Failed to create new in-memory System Table.");
			}

			/*
			 * Stop the old, remote, manager from accepting any more requests.
			 */
			try {
				systemTable.prepareForMigration(this.db.getURL().getURLwithRMIPort());
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "This System Table is already being migrated to another instance.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This System Table has already been migrated to another instance.");
			}

			/*
			 * Build the System Table's state from that of the existing table.
			 */
			try {
				newSystemTable.buildSystemTableState(systemTable);
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to migrate System Table to new machine.");
			} catch (MovedException e) {
				ErrorHandling
				.exceptionError(e,
				"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			} catch (SQLException e) {
				ErrorHandling.exceptionError(e, "Couldn't create persisted tables as expected.");
			} catch (NullPointerException e) {
				// ErrorHandling.exceptionError(e,
				// "Failed to migrate System Table to new machine. Machine has already been shut down.");
			}

			/*
			 * Shut down the old, remote, System Table. Redirect requests to new manager.
			 */
			try {
				systemTable.completeMigration();
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to complete migration.");
			} catch (MovedException e) {
				ErrorHandling
				.exceptionError(e,
				"This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
			}
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "System Table officially migrated to " + db.getURL().getDbLocation() + ".");

			this.systemTable = newSystemTable;
			db.getMetaDataReplicaManager().replicateMetaDataIfPossible(this, true); // replicate system table state.

		}

		/*
		 * Make the new System Table remotely accessible.
		 */
		this.isLocal = true;
		this.systemTableLocationURL = db.getURL();

		try {
			SystemTableRemote stub = (SystemTableRemote) UnicastRemoteObject.exportObject(systemTable, 0);

			getSystemTableRegistry().rebind(SCHEMA_MANAGER, stub);
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Binding System Table on port " + systemTableLocationURL.getRMIPort());
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "System Table migration failed.");
		}

		boolean successful = false;
		while (!successful) {
			try {
				successful = db.getRemoteInterface().setSystemTableLocationAsLocal();
			} catch (RemoteException e) {
				ErrorHandling.errorNoEvent("Failed to set the location of the System Table on the #(SM) machine.");
			}
		}

		try {
			systemTable.checkTableManagerAccessibility();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MovedException e) {
			e.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new System Table on " + db.getURL().getDbLocation() + ".");
		H2OEventBus.publish(new H2OEvent(db.getURL(), DatabaseStates.SYSTEM_TABLE_MIGRATION));
		return systemTable;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance ()
	 */
	public void migrateSystemTableToLocalInstance() {
	
		boolean persistedSchemaTablesExist = false;

		for (ReplicaSet rs : db.getAllTables()) {
			if (rs.getTableName().contains("H2O_") && rs.getLocalCopy() != null) {
				persistedSchemaTablesExist = true;
				break;
			}
		}

		migrateSystemTableToLocalInstance(persistedSchemaTablesExist, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#handleMovedException(org.h2. h2o.manager.MovedException)
	 */
	public void handleMovedException(MovedException e) throws SQLException {

		String newLocation = e.getMessage();

		if (newLocation == null) {
			throw new SQLException("The System Table has been shutdown. It must be re-instantiated before another query can be answered.");
		}

		systemTableLocationURL = DatabaseURL.parseURL(newLocation);
		Registry registry = getSystemTableRegistry();

		try {
			SystemTableRemote newSystemTable = (SystemTableRemote) registry.lookup(SCHEMA_MANAGER);
			this.systemTable = newSystemTable;
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "This System Table reference is old. It has been moved to: " + newLocation);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#setLookupLocation(uk.ac.standrews .cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public void setLookupLocation(IChordRemoteReference proxy) {
		this.lookupLocation = proxy;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#getLookupLocation()
	 */
	public IChordRemoteReference getLookupLocation() {
		return lookupLocation;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#lookup(java.lang.String)
	 */
	public TableManagerRemote lookup(String tableName, boolean useCache) throws SQLException {
		return lookup(new TableInfo(tableName), useCache);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#lookup(org.h2.h2o.util.TableInfo )
	 */
	public TableManagerRemote lookup(TableInfo tableInfo, boolean useCache) throws SQLException {
		if (tableInfo == null)
			return null;

		return lookup(tableInfo, false, useCache);
	}

	private TableManagerRemote lookup(TableInfo tableInfo, boolean alreadyCalled, boolean useCache) throws SQLException {
		/*
		 * The Table Manager may exist in one of two local caches, or it may have to be found via the Schema Manager. The caches are tested
		 * first, in the following order: CHECK ONE: Look in cache of Local Table Managers. CHECK TWO: The Table Manager is not local. Look
		 * in the cache of Remote Table Manager References. CHECK THREE: The Table Manager proxy is not known. Contact the System Table for
		 * the managers location.
		 */

		if (useCache) {
			/*
			 * CHECK ONE: Look in cache of Local Table Managers.
			 */
			TableManager tm = localTableManagers.get(tableInfo);

			if (tm != null)
				return tm;

			/*
			 * CHECK TWO: The Table Manager is not local. Look in the cache of Remote Table Manager References.
			 */
			TableManagerRemote tableManager = cachedTableManagerReferences.get(tableInfo);

			if (tableManager != null)
				return tableManager;
		}

		/*
		 * CHECK THREE: The Table Manager proxy is not known. Contact the System Table for the managers location.
		 */
		try {
			if (systemTable == null) {
				System.err.println("SYSTEM TABLE NULL");
				return makeAttemptToFindSystemTable(tableInfo, alreadyCalled); // Recursively calls lookup again if it hasn't tried to
				// already.
			}

			TableManagerWrapper tableManagerWrapper = systemTable.lookup(tableInfo);

			if (tableManagerWrapper == null)
				return null; // During a create table operation it is expected
			// that the lookup will return null here.

			// Put this Table Manager in the local cache then return it.
			TableManagerRemote tableManager = tableManagerWrapper.getTableManager();
			cachedTableManagerReferences.put(tableInfo, tableManager);

			return tableManager;
		} catch (MovedException e) {
			if (alreadyCalled)
				throw new SQLException("System failed to handle a MovedException correctly on a System Table lookup.");
			handleMovedException(e);
			return lookup(tableInfo, true, false);
		} catch (Exception e) {
			ErrorHandling.errorNoEvent("Error looking up System Table: " + e.getMessage());
			return makeAttemptToFindSystemTable(tableInfo, alreadyCalled);
		}
	}

	private TableManagerRemote makeAttemptToFindSystemTable(TableInfo tableInfo, boolean alreadyCalled) throws SQLException {
		if (alreadyCalled)
			throw new SQLException("Failed to find System Table. Query has been rolled back [location of request: " + this.db.getURL()
					+ ", ST location: " + this.systemTableLocationURL + "].");

		/*
		 * System Table no longer exists. Try to find new location.
		 */
		handleLostSystemTableConnection();
		return lookup(tableInfo, true, false);
	}

	/**
	 * The System Table connection has been lost. Try to connect to the System Table lookup location and obtain a reference to the new
	 * System Table.
	 * 
	 * @throws SQLException
	 */
	private void handleLostSystemTableConnection() throws SQLException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempting to fix a broken System Table connection.");

		try {
			IChordRemoteReference lookupLocation = null;

			int attempts = 0;
			do {
				try {
					lookupLocation = this.db.getChordInterface().getLookupLocation(SystemTableReference.systemTableKey);
				} catch (RemoteException e) {
					Thread.sleep(100); // wait, then try again.
				}
				attempts++;
			} while (lookupLocation == null && attempts < 10);

			String lookupHostname = lookupLocation.getRemote().getAddress().getHostName();
			int lookupPort = lookupLocation.getRemote().getAddress().getPort();

			lookForSystemTableReferenceViaChord(lookupHostname, lookupPort, false);

		} catch (Exception e) {
			throw new SQLException("Internal system error: failed to contact System Table.");
		}
	}

	private void lookForSystemTableReferenceViaChord(String hostname, int port, boolean alreadyCalled) throws RemoteException,
	NotBoundException, SQLException { // TODO change this method to rely on locator servers rather than chord.

		DatabaseInstanceRemote lookupInstance = null;

		DatabaseURL localURL = this.db.getURL();

		if (localURL.getHostname().equals(hostname) && localURL.getRMIPort() == port) {
			lookupInstance = this.db.getLocalDatabaseInstance();
		} else {
			IChordInterface chord = this.db.getChordInterface();

			lookupInstance = chord.getDatabaseInstanceAt(hostname, port);
		}

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
			DatabaseURL actualSystemTableLocation = lookupInstance.getSystemTableURL();

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL,
					"Actual System Table location found at: " + actualSystemTableLocation.getRMIPort());

			setSystemTableURL(actualSystemTableLocation);

			Registry registry = getSystemTableRegistry();

			this.systemTable = (SystemTableRemote) registry.lookup(SCHEMA_MANAGER);
			this.systemTableNode = systemTable.getChordReference();

		} catch (Exception e) {
			if (!alreadyCalled) {
				// System Table is not active anymore, and maintenance
				// mechanisms have not yet kicked in.
				SystemTableRemote newSystemTable = db.getRemoteInterface().reinstantiateSystemTable();

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempt to re-instantiate System Table: " + newSystemTable != null);

				if (newSystemTable != null) {
					systemTable = newSystemTable;
					// Now update the local references to this new System Table.
					DatabaseURL newSystemTableURL = getSystemTableURL();
					// Call this method again but with the new System Table
					// location.
					lookForSystemTableReferenceViaChord(newSystemTableURL.getHostname(), newSystemTableURL.getRMIPort(), true);
				}
			} else {
				throw new SQLException("Internal system error: failed to contact System Table.");
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#isThisSystemTableNode(uk.ac. standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public boolean isThisSystemTableNode(IChordRemoteReference otherNode) {
		if (systemTableNode == null)
			return false;
		return systemTableNode.equals(otherNode);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#addProxy(org.h2.h2o.comms.remote .TableManagerRemote)
	 */
	@Override
	public void addProxy(TableInfo tableInfo, TableManagerRemote tableManager) {
		this.cachedTableManagerReferences.remove(tableInfo);
		this.cachedTableManagerReferences.put(tableInfo, tableManager);

		// This is only ever called on the local machine, so it is okay to add
		// the Table Manager to the set of local table managers here.
		localTableManagers.put(tableInfo.getGenericTableInfo(), (TableManager) tableManager);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTableReference#addNewTableManagerReference( org.h2.h2o.util.TableInfo,
	 * org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	@Override
	public void addNewTableManagerReference(TableInfo ti, TableManagerRemote tm) {
		try {
			getSystemTable().changeTableManagerLocation(tm, ti);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MovedException e) {
			e.printStackTrace();
		}
		addProxy(ti, tm);
	}

	@Override
	public boolean addTableInformation(TableManagerRemote tableManagerRemote, TableInfo ti, Set<DatabaseInstanceWrapper> replicaLocations)
	throws RemoteException, MovedException, SQLException { // changed by
		// al
		localTableManagers.put(ti.getGenericTableInfo(), (TableManager) tableManagerRemote);

		return systemTable.addTableInformation(tableManagerRemote, ti, replicaLocations);
	}

	@Override
	public void removeTableInformation(TableInfo tableInfo) throws RemoteException, MovedException {
		localTableManagers.remove(tableInfo);
		cachedTableManagerReferences.remove(tableInfo);

		systemTable.removeTableInformation(tableInfo);
	}

	@Override
	public void removeAllTableInformation() throws RemoteException, MovedException {
		localTableManagers.clear();
		cachedTableManagerReferences.clear();

		systemTable.removeAllTableInformation();
	}

	public Map<TableInfo, TableManager> getLocalTableManagers() {
		return localTableManagers;
	}

	@Override
	public void pingHashLocation() {
		if (!isLocal) {
			ErrorHandling.hardError("Shouldn't be called when this isn't the System Table.");
		}

		try {
			db.getRemoteInterface().setSystemTableLocationAsLocal();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

}
