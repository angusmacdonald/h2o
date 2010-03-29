package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.h2.engine.Database;
import org.h2.h2o.remote.ChordInterface;
import org.h2.h2o.util.DatabaseURL;
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
public class SchemaManagerReference {

	/*
	 * SCHEMA MANAGER STATE.
	 */
	/**
	 * Reference to the database system's schema manager.
	 */
	private SchemaManagerRemote schemaManager;

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

	/**
	 * Get a reference to the schema manager. If the current schema manager location is
	 * not known this method will attempt to find it.
	 * 
	 * <p>The schema manager may be remote.
	 * @return
	 */
	public ISchemaManager getSchemaManager(){
		return getSchemaManager(false);
	}

	private SchemaManagerRemote getSchemaManager(boolean performedSchemaManagerLookup){
		if (schemaManager == null) {
			schemaManager = this.findSchemaManager();
			performedSchemaManagerLookup = true;
		}

		try {
			schemaManager.checkConnection();
		} catch (RemoteException e) {

			/*
			 * Call this method again if we attempted to access a cached schema manager reference and it didn't work.
			 */
			if (!performedSchemaManagerLookup){
				schemaManager = null;
				return getSchemaManager(true);
			}
			
			ErrorHandling.exceptionError(e, "Schema Manager is not accessible");
		} catch (MovedException e) {
			this.handleMovedException(e);
		}

		return schemaManager;
	}


	/**
	 * Get the location of the schema manager instance.
	 * 
	 * <p>This is the stored schema manager location (i.e. the system does not have to check whether the schema manager still exists at
	 * this location before returning a value).
	 * @return Stored schema manager location. 
	 */
	public DatabaseURL getSchemaManagerLocation(){
		return schemaManagerLocationURL;
	}

	/**
	 * True if the schema manager process is running locally.
	 */
	public boolean isSchemaManagerLocal(){
		return isLocal;
	}

	/**
	 * Attempts to find the schema manager by looking up its location in the RMI registry of
	 * the database instance which is responsible for the key range containing 'schema manager'.
	 * @return
	 */
	public SchemaManagerRemote findSchemaManager() {
		if (schemaManager != null){
			return schemaManager;
		}

		Registry registry = getSchemaManagerRegistry();

		try {
			schemaManager = (SchemaManagerRemote)registry.lookup("SCHEMA_MANAGER");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return schemaManager;
	}


	/**
	 * Returns a reference to the RMI registry of the schema manager.
	 * 
	 * <p>A lookup is performed to identify where the schema manager is currently located,
	 * then the registry is obtained.
	 * 
	 * <p>If the registry is not found this method returns null.
	 * @return	The RMI registry of this chord node.
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

	/**
	 * @param chord
	 */
	public void getSchemaManagerLocationIfNotKnown(ChordInterface chord) {
		if (schemaManagerLocationURL == null){ // true if this node has just joined a ring.
			schemaManagerLocationURL = chord.getActualSchemaManagerLocation();
		}
	}

	/**
	 * @param newSMLocation
	 */
	public void setNewSchemaManagerLocation(DatabaseURL newSMLocation) {
		if (newSMLocation.equals(db.getDatabaseURL())){ isLocal = true; }

		this.schemaManagerLocationURL = newSMLocation;
	}

	/**
	 * Provide a reference to the actual schema manager. This is typically called when a
	 * database has just been started.
	 */ 
	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}

	/**
	 * True if this instance has a reference to the schema manager.
	 */
	public boolean isConnectedToSM() {
		return (schemaManager != null);
	}

	/**
	 * Specify whether the schema manager lookup is in the keyrange of the given chord node.
	 */
	public void setInKeyRange(boolean inKeyRange) {
		this.inKeyRange = inKeyRange;
	}

	/**
	 * True if the schema manager chord lookup resolves to the local node. 
	 */
	public boolean isInKeyRange() {
		return inKeyRange;
	}

	public void migrateSchemaManagerToLocalInstance(boolean persistedSchemaTablesExist){
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

		DatabaseURL newLocation = db.getDatabaseURL();

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

		/*
		 * Confirm the new schema managers location by updating all local state.
		 */
		schemaManager = newSchemaManager;
		this.isLocal = true;
		this.schemaManagerLocationURL = newLocation;
		
		
		try {
			SchemaManagerRemote stub = (SchemaManagerRemote) UnicastRemoteObject.exportObject(schemaManager, 0);
			
			getSchemaManagerRegistry().bind("SCHEMA_MANAGER", stub);
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Schema manager migration failed.");
		}
		
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Finished building new schema manager on " + db.getDatabaseURL().getDbLocation() + ".");

	}

	/**
	 * 
	 */
	public void migrateSchemaManagerToLocalInstance() {

		boolean persistedSchemaTablesExist = false;

		for (ReplicaSet rs: db.getAllTables()){
			if (rs.getTableName().contains("H2O_") && rs.getLocalCopy() != null){
				persistedSchemaTablesExist = true;
				break;
			}
		}

		migrateSchemaManagerToLocalInstance(persistedSchemaTablesExist);
	}


	/**
	 * An exception has been thrown trying to access the schema manager because it has been moved to a new location. Handle this
	 * by updating the reference to that of the new schema manager.
	 */
	public void handleMovedException(MovedException e) {

		//TODO in places where this has been called it may be necessary to run the method again.

		//TODO finish implementing this method!
		String newLocation = e.getMessage();

		schemaManagerLocationURL = DatabaseURL.parseURL(newLocation);
		Registry registry = getSchemaManagerRegistry();

		try {
			SchemaManagerRemote newSchemaManager = (SchemaManagerRemote)registry.lookup("SCHEMA_MANAGER");
			this.schemaManager = newSchemaManager;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "This schema manager reference is old. It has been moved to: " + newLocation);

	}

	/**
	 * @param proxy
	 */
	public void setLookupLocation(IChordRemoteReference proxy) {
		this.lookupLocation = proxy;
	}

	/**
	 * @return
	 */
	public IChordRemoteReference getLookupLocation() {
		return lookupLocation;
	}
}
