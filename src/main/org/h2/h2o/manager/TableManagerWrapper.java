package org.h2.h2o.manager;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableManagerWrapper implements Serializable, Remote {

	private static final long serialVersionUID = -7088367740432999328L;

	/**
	 * Contains information on the table itself, such as its fully qualified name.
	 */
	private TableInfo tableInfo;
	
	/**
	 * Remote reference to the actual Table Manager.
	 */
	private TableManagerRemote tableManager;
	
	/**
	 * Location of the Table Manager.
	 */
	private DatabaseURL tableManagerURL;

	public TableManagerWrapper(TableInfo tableInfo, TableManagerRemote tableManager, DatabaseURL tableManagerURL) {
		this.tableInfo = tableInfo.getGenericTableInfo();
		this.tableManager = tableManager;
		this.tableManagerURL = tableManagerURL;
	}

	/**
	 * @return the tableInfo
	 */
	public TableInfo getTableInfo() throws RemoteException {
		return tableInfo;
	}

	/**
	 * @param tableInfo the tableInfo to set
	 */
	public void setTableInfo(TableInfo tableInfo) throws RemoteException {
		this.tableInfo = tableInfo.getGenericTableInfo();
	}

	/**
	 * @return the tableManager
	 */
	public TableManagerRemote getTableManager()throws RemoteException {
		return tableManager;
	}

	/**
	 * @param tableManager the tableManager to set
	 */
	public void setTableManager(TableManagerRemote tableManager)throws RemoteException {
		this.tableManager = tableManager;
	}

	/**
	 * @return the tableManagerURL
	 */
	public DatabaseURL getURL()throws RemoteException {
		return tableManagerURL;
	}

	/**
	 * @param tableManagerURL the tableManagerURL to set
	 */
	public void setTableManagerURL(DatabaseURL tableManagerURL)throws RemoteException {
		this.tableManagerURL = tableManagerURL;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return tableInfo.hashCode();
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
		TableManagerWrapper other = (TableManagerWrapper) obj;
		if (tableInfo == null) {
			if (other.tableInfo != null)
				return false;
		} else if (!tableInfo.equals(other.tableInfo))
			return false;
		return true;
	}

	/**
	 * @param localMachineLocation
	 * @return
	 */
	public boolean isLocalTo(DatabaseURL localMachineLocation) throws RemoteException{
		return tableManagerURL.equals(localMachineLocation);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TableManagerWrapper [tableInfo=" + tableInfo + ", tableManagerURL=" + tableManagerURL + "]";
	}
	
	
}
