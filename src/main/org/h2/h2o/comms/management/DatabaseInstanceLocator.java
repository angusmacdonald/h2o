package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.h2o.ChordInterface;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;

/**
 * 
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceLocator implements IDatabaseInstanceLocator {

	public static final String LOCAL_DATABASE_INSTANCE = "LOCAL_DB_INSTANCE";

	/**
	 * Local registry containing the reference to this database instance.
	 */
	private Registry localRegistry;
	
	private Registry schemaManagerRegistry;

	private DatabaseInstanceRemote localInstance;
	
	private Map<String, DatabaseInstanceRemote> databaseInstances;

	private ChordInterface chordManager;



	/**
	 * 
	 * @param localRegistry
	 * @param localInstance
	 */
	public DatabaseInstanceLocator(ChordInterface chordManager, DatabaseInstanceRemote localInstance) {
		this.chordManager = chordManager;
		this.localRegistry = chordManager.getLocalRegistry();
		this.schemaManagerRegistry = chordManager.getSchemaManagerRegistry();
		
		this.localInstance = localInstance;
		this.databaseInstances = new HashMap<String, DatabaseInstanceRemote>();

		DatabaseInstanceRemote stub = null;

		try {
			stub = (DatabaseInstanceRemote) UnicastRemoteObject.exportObject(localInstance, 0);
			localInstance = stub;
		} catch (RemoteException e) {
			e.printStackTrace();
		}


		try {
			this.localRegistry.bind(LOCAL_DATABASE_INSTANCE, stub);
			this.schemaManagerRegistry.bind(localInstance.getLocation().getUrlMinusSM(), stub);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public DatabaseInstanceRemote lookupDatabaseInstance(DatabaseURL databaseURL) throws SQLException{


		if (databaseInstances.containsKey(databaseURL.getUrlMinusSM())){
			return databaseInstances.get(databaseURL.getUrlMinusSM());
		}

		Registry schemaManager = chordManager.getSchemaManagerRegistry();

		DatabaseInstanceRemote remoteDatabaseInstance = null;
		try {
			remoteDatabaseInstance = (DatabaseInstanceRemote) schemaManager.lookup(databaseURL.getUrlMinusSM());
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
			databaseInstances.put(databaseURL.getUrlMinusSM(), remoteDatabaseInstance);
		}

		return remoteDatabaseInstance;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDatabaseInstanceLocator#removeLocalInstance()
	 */
	@Override
	public void removeLocalInstance() throws NotBoundException, RemoteException {
		try {

			localRegistry.unbind(LOCAL_DATABASE_INSTANCE);
		} catch (AccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	

}
