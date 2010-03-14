package org.h2.h2o.comms.management;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.remote.ChordInterface;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Controls access to data managers in the system. One instance of this class per database instance.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DataManagerLocator implements IDataManagerLocator{

	/**
	 * H2O. Data manager instances currently known to the local database instance.
	 */
	private Map<String, DataManagerRemote> dataManagers;
	private Registry registry;
	
	private ChordInterface chordManager;
	private SchemaManagerReference schemaManagerRef;

	/**
	 * Called to obtain a connection to the RMI registry.
	 * @param host	Host of the schema manager (which also hosts the RMI registry).
	 * @param port	Port where the schema manager is running (RMI port is this + 1, or defaults to a 20000 if in-memory).
	 * @throws RemoteException 
	 */
	public DataManagerLocator(ChordInterface chordManager, SchemaManagerReference schemaManagerRef) throws RemoteException {
		this.chordManager = chordManager;
		this.registry = chordManager.getLocalRegistry();
		this.schemaManagerRef = schemaManagerRef;
		
		dataManagers = new HashMap<String, DataManagerRemote>();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDataManagerLocator#lookupDataManager(java.lang.String)
	 */
	public DataManagerRemote lookupDataManager(String tableName) throws SQLException{

		DataManagerRemote dataManager = null;

		dataManager = dataManagers.get(tableName);

		if (dataManager != null){
			return dataManager;
		}
		//The local database doesn't have a reference to the requested DM. Find one via the schema manager registry.

		/*
		 * TODO this needs to have a reference to the schema manager RMI registry. 
		 */
		Registry schemaManagerRegistry = schemaManagerRef.getSchemaManagerRegistry();
		
		
		try {
			dataManager = (DataManagerRemote) schemaManagerRegistry.lookup(tableName);
		} catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, tableName + " was not bound to registry.");
			//Happens during the check before CREATE TABLE is run, so is sometimes acceptable.
		} catch (ClassCastException e){
			System.err.println(tableName);
			e.printStackTrace();
		}

		return dataManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDataManagerLocator#registerDataManager(org.h2.h2o.comms.DataManager)
	 */
	public void registerDataManager(DataManager dm){

		DataManagerRemote stub = null;

		DataManagerRemote dataManagerRemote = dm;
		try {
			stub = (DataManagerRemote) UnicastRemoteObject.exportObject(dataManagerRemote, 0);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {

			registry.rebind(dm.getTableName(), stub);

			dataManagers.put(dm.getTableName(), dm);
		}  catch (AccessException e) {
			e.printStackTrace();
		} catch (RemoteException e) {
			ErrorHandling.exceptionErrorNoEvent(e, "Lost contact with RMI registry when attempting to bind a manager.");
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.RMIServer#removeRegistryObject(java.lang.String)
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.management.IDataManagerLocator#removeRegistryObject(java.lang.String, boolean)
	 */
	@Override
	public void removeRegistryObject(String objectName, boolean removeLocalOnly) {
		try {
			DataManagerRemote dmr = lookupDataManager(objectName);
			if (dmr != null){
				dmr.removeDataManager();

			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			if (!removeLocalOnly) registry.unbind(objectName);
		} catch (NotBoundException e) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Table '" + objectName + "' was not bound (when trying to unbind).");
		} catch (AccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dataManagers.remove(objectName);
	}

}
