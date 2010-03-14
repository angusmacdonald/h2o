package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.h2.engine.Database;
import org.h2.h2o.remote.ChordInterface;
import org.h2.h2o.util.DatabaseURL;

/**
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
	 * @param databaseRemote
	 */
	public SchemaManagerReference(Database db) {
		this.db = db;
	}

	/**
	 * @param schemaManager2
	 * @param databaseRemote
	 */
	public SchemaManagerReference(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}

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
	
	
	public boolean isSchemaManagerLocal(){
		return isLocal;
	}

	public boolean isSchemaManagerLookupLocal() {
		return inKeyRange;
	}
	
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

		//		if (currentSMLocation == null){
		//			schemaManagerRegistryLocation = getActualSchemaManagerLocation();
		//		}


		try {
			remoteRegistry = LocateRegistry.getRegistry(schemaManagerLocationURL.getHostname(), schemaManagerLocationURL.getRMIPort());
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
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
	 * @param schemaManager
	 */
	public void setSchemaManager(SchemaManager schemaManager) {
		this.schemaManager = schemaManager;
	}

	/**
	 * @return
	 */
	public boolean isConnectedToSM() {
		return (schemaManager != null);
	}

	/**
	 * Specify whether the schema manager lookup is in the keyrange of the given chord node.
	 * @param b
	 */
	public void setInKeyRange(boolean inKeyRange) {
		this.inKeyRange = inKeyRange;
	}

	/**
	 * True if the schema manager key lookup resolves to a key in this nodes key range.
	 * @return
	 */
	public boolean isInKeyRange() {
		return inKeyRange;
	}


}
