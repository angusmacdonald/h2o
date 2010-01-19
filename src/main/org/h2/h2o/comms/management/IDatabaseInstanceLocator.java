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
//	 * Obtain a proxy for an exposed data manager.
//	 * @param instanceName	The name of the table whose data manager we are looking for.
//	 * @return	Reference to the exposed data manager (under remote interface).
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
//	 * Get references to all current data managers.
//	 * @return
//	 */
//	public abstract Set<DatabaseInstanceRemote> getInstances();
//
//	/**
//	 * @return
//	 */
//	public abstract DatabaseURL getSchemaManagerLocation();

}