package org.h2.h2o.manager;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DataManagerWrapper implements Serializable, Remote {

	private static final long serialVersionUID = -7088367740432999328L;

	/**
	 * Contains information on the table itself, such as its fully qualified name.
	 */
	private TableInfo tableInfo;
	
	/**
	 * Remote reference to the actual data manager.
	 */
	private DataManagerRemote dataManager;
	
	/**
	 * Location of the data manager.
	 */
	private DatabaseURL dataManagerURL;

	public DataManagerWrapper(TableInfo tableInfo, DataManagerRemote dataManager, DatabaseURL dataManagerURL) {
		this.tableInfo = tableInfo.getGenericTableInfo();
		this.dataManager = dataManager;
		this.dataManagerURL = dataManagerURL;
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
	 * @return the dataManager
	 */
	public DataManagerRemote getDataManager()throws RemoteException {
		return dataManager;
	}

	/**
	 * @param dataManager the dataManager to set
	 */
	public void setDataManager(DataManagerRemote dataManager)throws RemoteException {
		this.dataManager = dataManager;
	}

	/**
	 * @return the dataManagerURL
	 */
	public DatabaseURL getDataManagerURL()throws RemoteException {
		return dataManagerURL;
	}

	/**
	 * @param dataManagerURL the dataManagerURL to set
	 */
	public void setDataManagerURL(DatabaseURL dataManagerURL)throws RemoteException {
		this.dataManagerURL = dataManagerURL;
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
		DataManagerWrapper other = (DataManagerWrapper) obj;
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
		return dataManagerURL.equals(localMachineLocation);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataManagerWrapper [tableInfo=" + tableInfo + ", dataManagerURL=" + dataManagerURL + "]";
	}
	
	
}
