package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Database;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.remote.IChordInterface;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.SystemTableReplication;
import org.h2.h2o.util.TableInfo;
import org.h2.table.ReplicaSet;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.p2p.util.SHA1KeyFactory;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Encapsulates SystemTable references, containing state on whether the reference is local or remote,
 * whether the lookup is local or remote, and other relevant information. This class manages operations on
 * the System Table, such as migration between database instances.
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
	 * Reference to the remote chord node which is responsible for ensuring the System Table
	 * is running. This node is not necessarily the actual location of the System Table.
	 */
	private IChordRemoteReference lookupLocation;

	/**
	 * Key factory used to create keys for System Table lookup and to search for specific machines.
	 */
	private static SHA1KeyFactory keyFactory = new SHA1KeyFactory();

	/**
	 * The key of the System Table. This must be used in lookup operations to find the current location of the schema
	 * manager reference.
	 */
	public static IKey systemTableKey = keyFactory.generateKey("systemTable");

	/*
	 * GENERAL DATABASE.
	 */
	/**
	 * Reference to the local database instance. This is needed to get the local database URL, and to
	 * instantiate new System Table objects.
	 */
	private Database db;


	/**
	 * When a new object is created with this constructor the System Table reference may not exist, so only the database object
	 * is required.
	 * @param db
	 */
	public SystemTableReference(Database db) {
		this.db = db;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTable()
	 */
	public SystemTableRemote getSystemTable() {
		return getSystemTable(false);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTable(boolean)
	 */
	public SystemTableRemote getSystemTable(boolean inShutdown){
		return getSystemTable(false, inShutdown);
	}

	private SystemTableRemote getSystemTable(boolean performedSystemTableLookup, boolean inShutdown){
		try {

			if (systemTable == null) {
				performedSystemTableLookup = true;
				systemTable = this.findSystemTable();

			}


			systemTable.checkConnection();
		} catch (MovedException e) {
			if (!inShutdown){
				try {
					this.handleMovedException(e);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		} catch (SQLException e){
			return null;
		} catch (Exception e) {


			/*
			 * Call this method again if we attempted to access a cached System Table reference and it didn't work.
			 */
			if (!performedSystemTableLookup){
				systemTable = null;
				return getSystemTable(true, inShutdown);
			}

			ErrorHandling.exceptionError(e, "System Table is not accessible");
		}

		try {
			this.systemTableNode = systemTable.getChordReference();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return systemTable;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableLocation()
	 */
	public DatabaseURL getSystemTableURL(){
		return systemTableLocationURL;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#isSystemTableLocal()
	 */
	public boolean isSystemTableLocal(){
		return isLocal;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#findSystemTable()
	 */
	public SystemTableRemote findSystemTable() throws SQLException {
		if (systemTable != null){
			return systemTable;
		}

		Registry registry = getSystemTableRegistry();

		try {
			systemTable = (SystemTableRemote)registry.lookup(SCHEMA_MANAGER);
			this.systemTableNode = systemTable.getChordReference();
		} catch (Exception e) {
			throw new SQLException("Unable to find System Table.");
		}

		return systemTable;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#getSystemTableRegistry()
	 */
	public Registry getSystemTableRegistry(){
		Registry remoteRegistry = null;

		try {
			remoteRegistry = LocateRegistry.getRegistry(systemTableLocationURL.getHostname(), systemTableLocationURL.getRMIPort());

		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return remoteRegistry;

	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setNewSystemTableLocation(org.h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableURL(DatabaseURL newSMLocation) {
		if (newSMLocation.equals(db.getDatabaseURL())){ isLocal = true; }

		this.systemTableLocationURL = newSMLocation;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTableLocation(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference, org.h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableLocation(IChordRemoteReference systemTableLocation, DatabaseURL databaseURL) {
		this.systemTableNode = systemTableLocation;
		this.systemTableLocationURL = databaseURL;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTable(org.h2.h2o.manager.SystemTable)
	 */ 
	public void setSystemTable(SystemTable systemTable) {
		this.systemTable = systemTable;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setSystemTableLocationURL(org.h2.h2o.util.DatabaseURL)
	 */
	public void setSystemTableLocationURL(DatabaseURL databaseURL) {
		this.systemTableLocationURL = databaseURL;
	}
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#isConnectedToSM()
	 */
	public boolean isConnectedToSM() {
		return (systemTable != null);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setInKeyRange(boolean)
	 */
	public void setInKeyRange(boolean inKeyRange) {
		this.inKeyRange = inKeyRange;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#isInKeyRange()
	 */
	public boolean isInKeyRange() {
		return inKeyRange;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance(boolean, boolean)
	 */
	public void migrateSystemTableToLocalInstance(boolean persistedSchemaTablesExist, boolean recreateFromPersistedState){

		IChordRemoteReference oldSystemTableLocation = this.systemTableNode;
		
		if (recreateFromPersistedState){
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to re-instantiate System Table from persistent store.");
			/*
			 * INSTANTIATE A NEW System Table FROM PERSISTED STATE. This must be called if the previous System Table
			 * has failed.
			 */
			if (!persistedSchemaTablesExist){
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

				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "New System Table created.");
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
				systemTable.prepareForMigration(this.db.getDatabaseURL().getURLwithRMIPort());
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "This System Table is already being migrated to another instance.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This System Table has already been migrated to another instance.");
			}

			/*
			 * Build the System Table's state from that of the existing manager.
			 */
			try {
				newSystemTable.buildSystemTableState(systemTable);
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to migrate System Table to new machine.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			} catch (SQLException e) {
				ErrorHandling.exceptionError(e, "Couldn't create persisted tables as expected.");
			}


			/*
			 * Shut down the old, remote, System Table. Redirect requests to new manager.
			 */
			try {
				systemTable.completeMigration();
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to complete migration.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This shouldn't be possible here. The System Table has moved, but this instance should have had exclusive rights to it.");
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
			}
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "System Table officially migrated to " + db.getDatabaseURL().getDbLocation() + ".");

			this.systemTable = newSystemTable;
		}

		/*
		 * Make the new System Table remotely accessible.
		 */
		this.isLocal = true;
		this.systemTableLocationURL = db.getDatabaseURL();
		
		try {
			SystemTableRemote stub = (SystemTableRemote) UnicastRemoteObject.exportObject(systemTable, 0);

			getSystemTableRegistry().rebind(SCHEMA_MANAGER, stub);
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Binding System Table on port " + systemTableLocationURL.getRMIPort());
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "System Table migration failed.");
		}

		/*
		 * Replicate state to new successor.
		 */

		try {

			IChordRemoteReference successor = db.getChordInterface().getLocalChordReference().getRemote().getSuccessor();
			IChordRemoteReference localNode = db.getChordInterface().getLocalChordReference();
			//If the successor to this node is not itself (i.e. if this network has more than one node in it).
			if ( !successor.equals( localNode.getKey()) && !successor.equals(oldSystemTableLocation)){

				String hostname = db.getChordInterface().getLocalChordReference().getRemote().getSuccessor().getRemote().getAddress().getHostName();
				int port = db.getChordInterface().getLocalChordReference().getRemote().getSuccessor().getRemote().getAddress().getPort();
				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting System Table replication thread on : " + db.getDatabaseURL().getDbLocation() + ".");
				SystemTableReplication newThread = new SystemTableReplication(hostname, port, this, this.db.getChordInterface());
				newThread.start();
			} else {
				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "There is only one node in the network. There is no-where else to replicate the System Table.");
			}
		} catch (Exception e) {
			ErrorHandling.errorNoEvent("Failed to create replica for new System Table on its successor.");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new System Table on " + db.getDatabaseURL().getDbLocation() + ".");
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#migrateSystemTableToLocalInstance()
	 */
	public void migrateSystemTableToLocalInstance() {

		boolean persistedSchemaTablesExist = false;

		for (ReplicaSet rs: db.getAllTables()){
			if (rs.getTableName().contains("H2O_") && rs.getLocalCopy() != null){
				persistedSchemaTablesExist = true;
				break;
			}
		}

		migrateSystemTableToLocalInstance(persistedSchemaTablesExist, false);
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#handleMovedException(org.h2.h2o.manager.MovedException)
	 */
	public void handleMovedException(MovedException e) throws SQLException {

		String newLocation = e.getMessage();

		if (newLocation == null){
			throw new SQLException("The System Table has been shutdown. It must be re-instantiated before another query can be answered.");
		}

		systemTableLocationURL = DatabaseURL.parseURL(newLocation);
		Registry registry = getSystemTableRegistry();

		try {
			SystemTableRemote newSystemTable = (SystemTableRemote)registry.lookup(SCHEMA_MANAGER);
			this.systemTable = newSystemTable;
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "This System Table reference is old. It has been moved to: " + newLocation);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#setLookupLocation(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public void setLookupLocation(IChordRemoteReference proxy) {
		this.lookupLocation = proxy;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#getLookupLocation()
	 */
	public IChordRemoteReference getLookupLocation() {
		return lookupLocation;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#lookup(java.lang.String)
	 */
	public TableManagerRemote lookup(String tableName) throws SQLException {
		return lookup(new TableInfo(tableName));
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#lookup(org.h2.h2o.util.TableInfo)
	 */
	public TableManagerRemote lookup(TableInfo tableInfo) throws SQLException {
		return lookup(tableInfo, false);
	}

	private TableManagerRemote lookup(TableInfo tableInfo, boolean alreadyCalled) throws SQLException {

		TableManagerRemote tableManager = cachedTableManagerReferences.get(tableInfo);
		if (tableManager != null){

			try {
				tableManager.isAlive();

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Returning cached Table Manager for lookup operation.");

				return tableManager;
			} catch (RemoteException e) {
				//Lookup location again.
				cachedTableManagerReferences.remove(tableInfo);
			} catch (MovedException e) {
				cachedTableManagerReferences.remove(tableInfo);
				//Lookup location again.
			}	
		}

		try {
			tableManager = systemTable.lookup(tableInfo);
			
			cachedTableManagerReferences.put(tableInfo, tableManager);
			return tableManager;
		} catch (MovedException e) {
			if (alreadyCalled)
				throw new SQLException("System failed to handle a MovedException correctly on a System Table lookup.");
			handleMovedException(e);
			return lookup(tableInfo, true);
		} catch (Exception e) {
			if (alreadyCalled) throw new SQLException("Failed to find System Table. Query has been rolled back.");

			/*
			 * System Table no longer exists. Try to find new location.
			 */
			handleLostSystemTableConnection();
			return lookup(tableInfo, true);
		}
	}

	/**
	 * The System Table connection has been lost. Try to connect to the System Table lookup location
	 * and obtain a reference to the new System Table.
	 * @throws SQLException
	 */
	private void handleLostSystemTableConnection() throws SQLException {
		try {
			IChordRemoteReference lookupLocation = null;

			int attempts = 0;
			do {
				try{
					lookupLocation = this.db.getChordInterface().getLookupLocation(SystemTableReference.systemTableKey);
				} catch (RemoteException e){
					Thread.sleep(100); //wait, then try again.
				}
				attempts++;
			} while (lookupLocation == null && attempts < 10);



			DatabaseInstanceRemote lookupInstance  = null;

			if (this.db.getChordInterface().getLocalChordReference().equals(lookupLocation)){
				lookupInstance = this.db.getLocalDatabaseInstance();
			} else {
				String lookupHostname = lookupLocation.getRemote().getAddress().getHostName();
				int lookupPort = lookupLocation.getRemote().getAddress().getPort();

				IChordInterface chord = this.db.getChordInterface();
				lookupInstance = chord.getDatabaseInstanceAt(lookupHostname, lookupPort);
			}

			DatabaseURL actualSystemTableLocation = lookupInstance.getSystemTableURL();

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Actual System Table location found at: " + actualSystemTableLocation.getRMIPort());

			setSystemTableURL(actualSystemTableLocation);

			Registry registry = getSystemTableRegistry();


			this.systemTable = (SystemTableRemote)registry.lookup(SCHEMA_MANAGER);
			this.systemTableNode = systemTable.getChordReference();

		} catch (Exception e1) {
			e1.printStackTrace();
			throw new SQLException("Internal system error: failed to contact System Table.");
		} 
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#isThisSystemTableNode(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public boolean isThisSystemTableNode(IChordRemoteReference otherNode){
		if (systemTableNode == null) return false;
		return systemTableNode.equals(otherNode);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#addProxy(org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	@Override
	public void addProxy(TableInfo tableInfo, TableManagerRemote tableManager) {
		this.cachedTableManagerReferences.remove(tableInfo);
		this.cachedTableManagerReferences.put(tableInfo, tableManager);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTableReference#addNewTableManagerReference(org.h2.h2o.util.TableInfo, org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	@Override
	public void addNewTableManagerReference(TableInfo ti, TableManagerRemote tm) {
		try {
			db.getSystemTableReference().getSystemTable().changeTableManagerLocation(tm, ti);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (MovedException e) {
			e.printStackTrace();
		}
		db.getSystemTableReference().addProxy(ti, tm);
	}



}
