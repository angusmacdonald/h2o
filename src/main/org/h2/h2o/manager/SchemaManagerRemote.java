package org.h2.h2o.manager;

import java.rmi.RemoteException;

import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface SchemaManagerRemote extends ISchemaManager, Migratable {


	/**
	 * Stop the pinger thread from running on this schema manager. May mean the schema manager can't be found - should only
	 * be called if the database is being shut down.
	 */
	public void stopLookupPinger() throws RemoteException;


}
