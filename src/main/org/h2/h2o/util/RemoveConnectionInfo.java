package org.h2.h2o.util;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.SchemaManagerRemote;

/**
 * Run to remove connection information for a database instance. This is run as a thread rather than within the database process
 * to prevent deadlock on the local database instance.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RemoveConnectionInfo extends Thread {
	private SchemaManagerRemote schemaManager;
	private DatabaseInstanceRemote databaseInstanceRemote;

	/**
	 * @param schemaManager
	 * @param databaseInstanceRemote 
	 */
	public RemoveConnectionInfo(SchemaManagerRemote schemaManager, DatabaseInstanceRemote databaseInstanceRemote) {
		this.schemaManager = schemaManager;
		this.databaseInstanceRemote = databaseInstanceRemote;
	}

	public void run(){
		try {
			schemaManager.removeConnectionInformation(databaseInstanceRemote);
		} catch (Exception e) {
			//Doesn't matter.
		}
	}
}
