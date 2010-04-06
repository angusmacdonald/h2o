package org.h2.h2o.comms.remote;

import java.io.Serializable;


/**
 * Wrapper for remote database instance proxies. Contains a reference to the proxy itself
 * and whether the database is actually alive.
 * 
 * <p>This is done because connection information is maintained in the schema manager even when a connection itself
 * has become inactive.
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
	 * Wether the database instance this proxy points to is still active.
	 */
	public boolean isActive() {
		return active;
	}

	/**
	 * Set the database instance as active (allowing incoming connections) or inactive (not running).
	 */
	public void setActive(boolean active) {
		this.active = active;
	}

/**
 * @return Remote proxy for a database instances interface.
 */
	public DatabaseInstanceRemote getDatabaseInstance() {
		return databaseInstance;
	}
	
	
}
