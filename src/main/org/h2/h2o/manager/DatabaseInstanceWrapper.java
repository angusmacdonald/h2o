package org.h2.h2o.manager;

import java.io.Serializable;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceWrapper implements Serializable {

	private static final long serialVersionUID = 9193285872031823819L;

	private DatabaseInstanceRemote databaseInstance;
	
	private boolean active = true;

	/**
	 * @param databaseInstance	Reference to the local database instance.
	 * @param active			Whether the database is currently active.
	 */
	public DatabaseInstanceWrapper(DatabaseInstanceRemote databaseInstance,
			boolean active) {
		super();
		this.databaseInstance = databaseInstance;
		this.active = active;
	}

	/**
	 * @return the active
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * @param active the active to set
	 */
	public void setActive(boolean active) {
		this.active = active;
	}

	/**
	 * @return the databaseInstance
	 */
	public DatabaseInstanceRemote getDatabaseInstance() {
		return databaseInstance;
	}
	
	
}
