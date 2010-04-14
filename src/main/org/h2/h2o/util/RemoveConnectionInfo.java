package org.h2.h2o.util;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.SystemTableRemote;

/**
 * Run to remove connection information for a database instance. This is run as a thread rather than within the database process
 * to prevent deadlock on the local database instance.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RemoveConnectionInfo extends Thread {
	private SystemTableRemote systemTable;
	private DatabaseInstanceRemote databaseInstanceRemote;

	/**
	 * @param systemTable
	 * @param databaseInstanceRemote 
	 */
	public RemoveConnectionInfo(SystemTableRemote systemTable, DatabaseInstanceRemote databaseInstanceRemote) {
		this.systemTable = systemTable;
		this.databaseInstanceRemote = databaseInstanceRemote;
	}

	public void run(){
		try {
			systemTable.removeConnectionInformation(databaseInstanceRemote);
		} catch (Exception e) {
			//Doesn't matter.
		}
	}
}
