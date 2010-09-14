package org.h2o.db.manager;

import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.interfaces.SystemTableRemote;

public class SystemTableWrapper {
	private SystemTableRemote systemTable;
	private DatabaseURL url;

	public SystemTableWrapper(SystemTableRemote systemTable, DatabaseURL url) {
		this.systemTable = systemTable;
		this.url = url;
	}

	public SystemTableWrapper() { } //All information may not be known at startup.

	public SystemTableRemote getSystemTable() {
		return systemTable;
	}

	public DatabaseURL getURL() {
		return url;
	}

	public void setSystemTable(SystemTableRemote systemTable) {
		this.systemTable = systemTable;
	}

	public void setURL(DatabaseURL url) {
		this.url = url;	
	}
}
