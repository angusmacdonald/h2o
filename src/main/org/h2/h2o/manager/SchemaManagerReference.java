package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.h2.engine.Database;
import org.h2.h2o.remote.ChordInterface;
import org.h2.h2o.util.DatabaseURL;

/**
 * Encapsulates SchemaManager references, containing state on whether the reference is local or remote,
 * whether the lookup is local or remote, and other relevant information. This class manages operations on
 * the Schema Manager, such as migration between database instances.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerReference {

	/**
	 * Reference to the database system's schema manager.
	 */
	private ISchemaManager schemaManager;

	/**
	 * Location of the actual schema manager.
	 */
	private DatabaseURL schemaManagerLocationURL = null;
	
	/**
	 * Whether the schema manager is running on this node.
	 */
	private boolean isLocal = false;

	private boolean inKeyRange = false;

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
		if (schemaManager == null) schemaManager = this.findSchemaManager();
		
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
	public ISchemaManager findSchemaManager() {
		if (schemaManager != null){
			return schemaManager;
		}

		Registry registry = getSchemaManagerRegistry();

		try {
			schemaManager = (ISchemaManager)registry.lookup("SCHEMA_MANAGER");
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


}
