package org.h2.h2o.comms.management;

import java.sql.SQLException;

import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.remote.DataManagerRemote;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDataManagerLocator {

	/**
	 * Obtain a proxy for an exposed data manager.
	 * @param tableName	The name of the table whose data manager we are looking for.
	 * @return	Reference to the exposed data manager (under remote interface).
	 */
	public abstract DataManagerRemote lookupDataManager(String tableName)
			throws SQLException;

	/**
	 * Register the local DM interface with the global RMI registry.
	 * @param interfaceName	Name given to the DM interface on the registry. 
	 * @param dm The data manager instance to be exposed.
	 */
	public abstract void registerDataManager(DataManager dm);

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	public abstract void removeRegistryObject(String objectName,
			boolean removeLocalOnly);

}