package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.h2o.ChordInterface;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * 
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceLocator implements IDatabaseInstanceLocator {

	public static final String LOCAL_DATABASE_INSTANCE = "LOCAL_DB_INSTANCE";

	private static final String DATABASE_INSTANCE_PREFIX = "DB_INSTANCE_";

	/**
	 * Local registry containing the reference to this database instance.
	 */
	private Registry localRegistry;

	private DatabaseInstanceRemote localInstance;

	private Map<String, DatabaseInstanceRemote> locallyCachedDatabaseInstances;

	private ChordInterface chord;

	private String[] cachedItemsInSchemaManager;

	/**
	 * 
	 * @param localRegistry
	 * @param localInstance
	 */
	public DatabaseInstanceLocator(ChordInterface chordManager, DatabaseInstanceRemote localInstance) {
		this.chord = chordManager;
		this.localRegistry = chordManager.getLocalRegistry();

		this.localInstance = localInstance;
		this.locallyCachedDatabaseInstances = new HashMap<String, DatabaseInstanceRemote>();

		DatabaseInstanceRemote stub = null;

		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(localInstance, 0);
			localInstance = stub;
		} catch (RemoteException e) {
			e.printStackTrace();
		}


		try {
			this.localRegistry.bind(LOCAL_DATABASE_INSTANCE, stub);
			chordManager.getSchemaManagerRegistry().bind(DATABASE_INSTANCE_PREFIX + localInstance.getLocation().getUrlMinusSM(), stub);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public DatabaseInstanceRemote lookupDatabaseInstance(DatabaseURL databaseURL) throws SQLException{

		if (locallyCachedDatabaseInstances.containsKey(databaseURL.getUrlMinusSM())){
			return locallyCachedDatabaseInstances.get(databaseURL.getUrlMinusSM());
		}

		Registry schemaManager = chord.getSchemaManagerRegistry();

		DatabaseInstanceRemote remoteDatabaseInstance = null;
		try {
			remoteDatabaseInstance = (DatabaseInstanceRemote) schemaManager.lookup(DATABASE_INSTANCE_PREFIX + databaseURL.getUrlMinusSM());
		} catch (AccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (remoteDatabaseInstance != null){
			locallyCachedDatabaseInstances.put(DATABASE_INSTANCE_PREFIX + databaseURL.getUrlMinusSM(), remoteDatabaseInstance);
		}

		return remoteDatabaseInstance;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDatabaseInstanceLocator#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceRemote> getDatabaseInstances() {
		Registry schemaManagerRegistry = chord.getSchemaManagerRegistry();

		Set<DatabaseInstanceRemote> databaseInstances = new HashSet<DatabaseInstanceRemote>();


		String[] itemsInSchemaManager = null;
		try {
			itemsInSchemaManager = schemaManagerRegistry.list();
		} catch (Exception e) {
			ErrorHandling.exceptionError(e, "Failed to connect to the schema manager.");
		}

		/*
		 * First check whether all of the items in the schema manager are cached locally. If they are, just return the cache.
		 */
		if (itemsInSchemaManager.equals(cachedItemsInSchemaManager)){
			return new HashSet<DatabaseInstanceRemote>(locallyCachedDatabaseInstances.values());
		}

		cachedItemsInSchemaManager = itemsInSchemaManager;

		/*
		 * We know that the schema manager has at least some new items. In this case we want to get refererences to all
		 * of these items, update the cache, and return a set of instances.
		 */
		for (int i = 0; i < cachedItemsInSchemaManager.length; i++){
			if (cachedItemsInSchemaManager[i].startsWith(DATABASE_INSTANCE_PREFIX)){
				try {
					DatabaseInstanceRemote instance = (DatabaseInstanceRemote) schemaManagerRegistry.lookup(cachedItemsInSchemaManager[i]);

					databaseInstances.add(instance);
					locallyCachedDatabaseInstances.put(cachedItemsInSchemaManager[i], instance);
				} catch (NotBoundException e) {
					ErrorHandling.exceptionErrorNoEvent(e, "Failed to obtain one of the database instances which was thought to be in the schema manager : " + cachedItemsInSchemaManager[i]);
				} catch (AccessException e) {
					ErrorHandling.exceptionErrorNoEvent(e, "Failed in lookup to the schema manager registry " + schemaManagerRegistry);
				} catch (RemoteException e) {
					ErrorHandling.exceptionErrorNoEvent(e, "Failed in lookup to the schema manager registry " + schemaManagerRegistry);
				}
			}
		}

		return databaseInstances;
	}	



	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDatabaseInstanceLocator#removeLocalInstance()
	 */
	@Override
	public void removeLocalInstance() throws NotBoundException, RemoteException {

		localRegistry.unbind(LOCAL_DATABASE_INSTANCE);

	}
}
