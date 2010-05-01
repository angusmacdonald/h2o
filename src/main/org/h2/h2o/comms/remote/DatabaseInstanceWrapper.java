package org.h2.h2o.comms.remote;

import java.io.Serializable;

import org.h2.h2o.util.DatabaseURL;


/**
 * Wrapper for remote database instance proxies. Contains a reference to the proxy itself
 * and whether the database is actually alive.
 * 
 * <p>This is done because connection information is maintained in the System Table even when a connection itself
 * has become inactive.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceWrapper implements Serializable {

	private static final long serialVersionUID = 9193285872031823819L;

	private DatabaseURL databaseURL;

	private DatabaseInstanceRemote databaseInstance;

	private boolean active = true;

	/**
	 * @param databaseURL 		The location of this database instance.
	 * @param databaseInstance	Reference to the local database instance.
	 * @param active			Whether the database is currently active.
	 */
	public DatabaseInstanceWrapper(DatabaseURL databaseURL, DatabaseInstanceRemote databaseInstance,
			boolean active) {
		super();
		this.databaseURL = databaseURL;
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

	/**
	 * @return
	 */
	public DatabaseURL getDatabaseURL() {
		return databaseURL;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((databaseInstance == null) ? 0 : databaseInstance.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseInstanceWrapper other = (DatabaseInstanceWrapper) obj;
		if (databaseInstance == null) {
			if (other.databaseInstance != null)
				return false;
		} else if (!databaseInstance.equals(other.databaseInstance))
			return false;
		return true;
	}
	
	
	


}
