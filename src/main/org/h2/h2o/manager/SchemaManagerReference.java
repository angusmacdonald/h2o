package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.remote.IChordInterface;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.SchemaManagerReplication;
import org.h2.h2o.util.TableInfo;
import org.h2.table.ReplicaSet;

import uk.ac.standrews.cs.nds.p2p.interfaces.IKey;
import uk.ac.standrews.cs.nds.p2p.util.SHA1KeyFactory;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * Encapsulates SchemaManager references, containing state on whether the reference is local or remote,
 * whether the lookup is local or remote, and other relevant information. This class manages operations on
 * the Schema Manager, such as migration between database instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerReference implements ISchemaManagerReference {

	/**
	 * Name under which the schema manager is located in the local RMI registry.
	 */
	public static final String SCHEMA_MANAGER = "SCHEMA_MANAGER";

	/*
	 * SCHEMA MANAGER STATE.
	 */
	/**
	 * Reference to the database system's schema manager.
	 */
	private SchemaManagerRemote schemaManager;

	private Map<TableInfo, DataManagerRemote> cachedDataManagerReferences = new HashMap<TableInfo, DataManagerRemote>();

	/**
	 * Location of the actual schema manager.
	 */
	private DatabaseURL schemaManagerLocationURL = null;

	/**
	 * Whether the schema manager is running on this node.
	 */
	private boolean isLocal = false;

	/**
	 * Whether the schema manager lookup on Chord resolves to this machines keyspace.
	 */
	private boolean inKeyRange = false;

	private IChordRemoteReference schemaManagerNode;

	/*
	 * CHORD-RELATED.
	 */
	/**
	 * Reference to the remote chord node which is responsible for ensuring the schema manager
	 * is running. This node is not necessarily the actual location of the schema manager.
	 */
	private IChordRemoteReference lookupLocation;

	/**
	 * Key factory used to create keys for schema manager lookup and to search for specific machines.
	 */
	private static SHA1KeyFactory keyFactory = new SHA1KeyFactory();

	/**
	 * The key of the schema manager. This must be used in lookup operations to find the current location of the schema
	 * manager reference.
	 */
	public static IKey schemaManagerKey = keyFactory.generateKey("schemaManager");

	/*
	 * GENERAL DATABASE.
	 */
	/**
	 * Reference to the local database instance. This is needed to get the local database URL, and to
	 * instantiate new Schema Manager objects.
	 */
	private Database db;


	/**
	 * When a new object is created with this constructor the schema manager reference may not exist, so only the database object
	 * is required.
	 * @param db
	 */
	public SchemaManagerReference(Database db) {
		this.db = db;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#getSchemaManager()
	 */
	public SchemaManagerRemote getSchemaManager() {
		return getSchemaManager(false);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#getSchemaManager(boolean)
	 */
	public SchemaManagerRemote getSchemaManager(boolean inShutdown){
		return getSchemaManager(false, inShutdown);
	}

	private SchemaManagerRemote getSchemaManager(boolean performedSchemaManagerLookup, boolean inShutdown){
		try {

			if (schemaManager == null) {
				performedSchemaManagerLookup = true;
				schemaManager = this.findSchemaManager();

			}


			schemaManager.checkConnection();
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
			 * Call this method again if we attempted to access a cached schema manager reference and it didn't work.
			 */
			if (!performedSchemaManagerLookup){
				schemaManager = null;
				return getSchemaManager(true, inShutdown);
			}

			ErrorHandling.exceptionError(e, "Schema Manager is not accessible");
		}

		try {
			this.schemaManagerNode = schemaManager.getChordReference();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return schemaManager;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#getSchemaManagerLocation()
	 */
	public DatabaseURL getSchemaManagerURL(){
		return schemaManagerLocationURL;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#isSchemaManagerLocal()
	 */
	public boolean isSchemaManagerLocal(){
		return isLocal;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#findSchemaManager()
	 */
	public SchemaManagerRemote findSchemaManager() throws SQLException {
		if (schemaManager != null){
			return schemaManager;
		}

		Registry registry = getSchemaManagerRegistry();

		try {
			schemaManager = (SchemaManagerRemote)registry.lookup(SCHEMA_MANAGER);
			this.schemaManagerNode = schemaManager.getChordReference();
		} catch (Exception e) {
			throw new SQLException("Unable to find schema manager.");
		}

		return schemaManager;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#getSchemaManagerRegistry()
	 */
	public Registry getSchemaManagerRegistry(){
		Registry remoteRegistry = null;

		try {
			remoteRegistry = LocateRegistry.getRegistry(schemaManagerLocationURL.getHostname(), schemaManagerLocationURL.getRMIPort());

		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return remoteRegistry;

	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setNewSchemaManagerLocation(org.h2.h2o.util.DatabaseURL)
	 */
	public void setSchemaManagerURL(DatabaseURL newSMLocation) {
		if (newSMLocation.equals(db.getDatabaseURL())){ isLocal = true; }

		this.schemaManagerLocationURL = newSMLocation;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setSchemaManagerLocation(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference, org.h2.h2o.util.DatabaseURL)
	 */
	public void setSchemaManagerLocation(IChordRemoteReference schemaManagerLocation, DatabaseURL databaseURL) {
		this.schemaManagerNode = schemaManagerLocation;
		this.schemaManagerLocationURL = databaseURL;
	}
	
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setSchemaManager(org.h2.h2o.manager.SchemaManager)
	 */ 
	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setSchemaManagerLocationURL(org.h2.h2o.util.DatabaseURL)
	 */
	public void setSchemaManagerLocationURL(DatabaseURL databaseURL) {
		this.schemaManagerLocationURL = databaseURL;
	}
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#isConnectedToSM()
	 */
	public boolean isConnectedToSM() {
		return (schemaManager != null);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setInKeyRange(boolean)
	 */
	public void setInKeyRange(boolean inKeyRange) {
		this.inKeyRange = inKeyRange;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#isInKeyRange()
	 */
	public boolean isInKeyRange() {
		return inKeyRange;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#migrateSchemaManagerToLocalInstance(boolean, boolean)
	 */
	public void migrateSchemaManagerToLocalInstance(boolean persistedSchemaTablesExist, boolean recreateFromPersistedState){

		if (recreateFromPersistedState){
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to re-instantiate schema manager from persistent store.");
			/*
			 * INSTANTIATE A NEW SCHEMA MANAGER FROM PERSISTED STATE. This must be called if the previous schema manager
			 * has failed.
			 */
			if (!persistedSchemaTablesExist){
				ErrorHandling.hardError("The system doesn't have a mechanism for recreating the state of the schema manager from remote machines.");
			}

			SchemaManagerRemote newSchemaManager = null;
			try {
				newSchemaManager = new SchemaManager(db, false); // false - don't overwrite saved persisted state.
			} catch (Exception e) {
				ErrorHandling.exceptionError(e, "Failed to create new in-memory schema manager.");
			}

			try {
				newSchemaManager.buildSchemaManagerState();

				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "New schema manager created.");
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				ErrorHandling.exceptionError(e, "Persisted state didn't exist on machine as expected.");
			}

			schemaManager = newSchemaManager;

		} else {
			/*
			 * CREATE A NEW SCHEMA MANAGER BY COPYING THE STATE OF THE CURRENT ACTIVE IN-MEMORY SCHEMA MANAGER.
			 */
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate schema manager.");

			/*
			 * Create a new schema manager instance locally.
			 */
			SchemaManagerRemote newSchemaManager = null;
			try {
				newSchemaManager = new SchemaManager(db, true);
			} catch (Exception e) {
				ErrorHandling.exceptionError(e, "Failed to create new in-memory schema manager.");
			}

			/*
			 * Stop the old, remote, manager from accepting any more requests.
			 */
			try {
				schemaManager.prepareForMigration(this.db.getDatabaseURL().getURLwithRMIPort());
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "This schema manager is already being migrated to another instance.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This schema manager has already been migrated to another instance.");
			}

			/*
			 * Build the schema manager's state from that of the existing manager.
			 */
			try {
				newSchemaManager.buildSchemaManagerState(schemaManager);
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to migrate schema manager to new machine.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This shouldn't be possible here. The schema manager has moved, but this instance should have had exclusive rights to it.");
			} catch (SQLException e) {
				ErrorHandling.exceptionError(e, "Couldn't create persisted tables as expected.");
			}

			/*
			 * Shut down the old, remote, schema manager. Redirect requests to new manager.
			 */
			try {
				schemaManager.completeMigration();
			} catch (RemoteException e) {
				ErrorHandling.exceptionError(e, "Failed to complete migration.");
			} catch (MovedException e) {
				ErrorHandling.exceptionError(e, "This shouldn't be possible here. The schema manager has moved, but this instance should have had exclusive rights to it.");
			} catch (MigrationException e) {
				ErrorHandling.exceptionError(e, "Migration process timed out. It took too long.");
			}
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Schema Manager officially migrated to " + db.getDatabaseURL().getDbLocation() + ".");

			this.schemaManager = newSchemaManager;
		}

		/*
		 * Confirm the new schema managers location by updating all local state.
		 */

		this.isLocal = true;
		this.schemaManagerLocationURL = db.getDatabaseURL();


		try {
			SchemaManagerRemote stub = (SchemaManagerRemote) UnicastRemoteObject.exportObject(schemaManager, 0);

			getSchemaManagerRegistry().bind(SCHEMA_MANAGER, stub);
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Binding schema manager on port " + schemaManagerLocationURL.getRMIPort());
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Schema manager migration failed.");
		}

		/*
		 * Replicate state to new successor.
		 */

		try {
			String hostname = db.getChordInterface().getLocalChordReference().getRemote().getSuccessor().getRemote().getAddress().getHostName();
			int port = db.getChordInterface().getLocalChordReference().getRemote().getSuccessor().getRemote().getAddress().getPort();
			SchemaManagerReplication newThread = new SchemaManagerReplication(hostname, port, this.db.getSchemaManager(), this.db.getChordInterface());
			newThread.start();
		} catch (RemoteException e) {
			ErrorHandling.errorNoEvent("Failed to create replica for new schema manager on its successor.");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new schema manager on " + db.getDatabaseURL().getDbLocation() + ".");
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#migrateSchemaManagerToLocalInstance()
	 */
	public void migrateSchemaManagerToLocalInstance() {

		boolean persistedSchemaTablesExist = false;

		for (ReplicaSet rs: db.getAllTables()){
			if (rs.getTableName().contains("H2O_") && rs.getLocalCopy() != null){
				persistedSchemaTablesExist = true;
				break;
			}
		}

		migrateSchemaManagerToLocalInstance(persistedSchemaTablesExist, false);
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#handleMovedException(org.h2.h2o.manager.MovedException)
	 */
	public void handleMovedException(MovedException e) throws SQLException {

		String newLocation = e.getMessage();

		if (newLocation == null){
			throw new SQLException("The schema manager has been shutdown. It must be re-instantiated before another query can be answered.");
		}

		schemaManagerLocationURL = DatabaseURL.parseURL(newLocation);
		Registry registry = getSchemaManagerRegistry();

		try {
			SchemaManagerRemote newSchemaManager = (SchemaManagerRemote)registry.lookup(SCHEMA_MANAGER);
			this.schemaManager = newSchemaManager;
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "This schema manager reference is old. It has been moved to: " + newLocation);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#setLookupLocation(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public void setLookupLocation(IChordRemoteReference proxy) {
		this.lookupLocation = proxy;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#getLookupLocation()
	 */
	public IChordRemoteReference getLookupLocation() {
		return lookupLocation;
	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#lookup(java.lang.String)
	 */
	public DataManagerRemote lookup(String tableName) throws SQLException {
		return lookup(new TableInfo(tableName));
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#lookup(org.h2.h2o.util.TableInfo)
	 */
	public DataManagerRemote lookup(TableInfo tableInfo) throws SQLException {
		return lookup(tableInfo, false);
	}

	private DataManagerRemote lookup(TableInfo tableInfo, boolean alreadyCalled) throws SQLException {

		DataManagerRemote dataManager = cachedDataManagerReferences.get(tableInfo);
		if (dataManager != null){

			try {
				dataManager.testAvailability();

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Returning cached data manager for lookup operation.");

				return dataManager;
			} catch (RemoteException e) {
				//Lookup location again.
			} catch (MovedException e) {
				//Lookup location again.
			}	
		}

		try {
			dataManager = schemaManager.lookup(tableInfo);
			cachedDataManagerReferences.put(tableInfo, dataManager);
			return dataManager;
		} catch (MovedException e) {
			if (alreadyCalled)
				throw new SQLException("System failed to handle a MovedException correctly on a schema manager lookup.");
			handleMovedException(e);
			return lookup(tableInfo, true);
		} catch (RemoteException e) {
			if (alreadyCalled) throw new SQLException("Failed to find Schema Manager. Query has been rolled back.");

			/*
			 * Schema manager no longer exists. Try to find new location.
			 */
			handleLostSchemaManagerConnection();
			return lookup(tableInfo, true);
		}
	}

	/**
	 * The schema manager connection has been lost. Try to connect to the schema manager lookup location
	 * and obtain a reference to the new schema manager.
	 * @throws SQLException
	 */
	private void handleLostSchemaManagerConnection() throws SQLException {
		try {
			IChordRemoteReference lookupLocation = null;

			int attempts = 0;
			do {
				try{
					lookupLocation = this.db.getChordInterface().getLookupLocation(SchemaManagerReference.schemaManagerKey);
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

			DatabaseURL actualSchemaManagerLocation = lookupInstance.getSchemaManagerLocation();

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Actual schema manager location found at: " + actualSchemaManagerLocation.getRMIPort());

			setSchemaManagerURL(actualSchemaManagerLocation);

			Registry registry = getSchemaManagerRegistry();


			this.schemaManager = (SchemaManagerRemote)registry.lookup(SCHEMA_MANAGER);
			this.schemaManagerNode = schemaManager.getChordReference();

		} catch (Exception e1) {
			e1.printStackTrace();
			throw new SQLException("Internal system error: failed to contact Schema Manager.");
		} 
	}
//
//	/**
//	 * The schema manager has failed or been shut down. This node must know figure out where the schema manager's persisted state
//	 * was, and use that state to create a new schema manager.
//	 * @return
//	 */
//	private DataManagerRemote handleFailedSchemaManager() {
//
//		boolean unstable = true;
//		do{
//			try {
//				IChordRemoteReference chordNode = this.db.getChordInterface().lookupSchemaManagerNodeLocation();
//			} catch (RemoteException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//		} while (unstable);
//		return null;
//	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#shutdown()
	 */
	public void shutdown() {
		if(inKeyRange && !Constants.IS_NON_SM_TEST){
			try {
				schemaManager.shutdown(true);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManagerReference#isThisSchemaManagerNode(uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference)
	 */
	public boolean isThisSchemaManagerNode(IChordRemoteReference otherNode){
		return schemaManagerNode.equals(otherNode);
	}



}
