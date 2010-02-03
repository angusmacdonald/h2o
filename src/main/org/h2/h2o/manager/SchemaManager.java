package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManager implements ISchemaManager {

	/**
	 * Interface to the in-memory state of the schema manager.
	 */
	private ISchemaManager inMemory;

	/**
	 * Interface to the persisted state of this schema manager. This object interacts
	 * with the database to store the state of the schema manager on disk.
	 */
	private ISchemaManager persisted;

	public SchemaManager(Database db, boolean persistedSchemaTablesExist) {

		try {
			this.inMemory = new InMemorySchemaManager(db);
			this.persisted = new PersistentSchemaManager(db, persistedSchemaTablesExist);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/******************************************************************
	 ****	Methods which require both in memory and persisted data structures to be updated.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addConnectionInformation(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL)
	throws RemoteException {
		inMemory.addConnectionInformation(databaseURL);
		return persisted.addConnectionInformation(databaseURL);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo ti) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to add a single replica to the system: " + ti);

		inMemory.addReplicaInformation(ti);
		persisted.addReplicaInformation(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.DataManagerRemote, org.h2.h2o.TableInfo)
	 */
	@Override
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails) throws RemoteException {

		boolean result = inMemory.addTableInformation(dataManager, tableDetails);
		persisted.addTableInformation(dataManager, tableDetails);

		return result;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void removeReplicaInformation(TableInfo ti) throws RemoteException {
		inMemory.removeReplicaInformation(ti);
		persisted.removeReplicaInformation(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeTableInformation(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean removeTableInformation(TableInfo ti) throws RemoteException {
		boolean result = inMemory.removeTableInformation(ti);
		persisted.removeTableInformation(ti);

		return result;
	}

	/******************************************************************
	 ****	Methods which only require checking in memory data structures.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException {
		return inMemory.exists(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getAllTablesInSchema(java.lang.String)
	 */
	@Override
	public Set<String> getAllTablesInSchema(String schemaName)
	throws RemoteException {
		return inMemory.getAllTablesInSchema(schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNewTableSetNumber()
	 */
	@Override
	public int getNewTableSetNumber() throws RemoteException {
		return inMemory.getNewTableSetNumber();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNumberofReplicas(java.lang.String, java.lang.String)
	 */
	@Override
	public int getNumberofReplicas(String tableName, String schemaName)
	throws RemoteException {
		return inMemory.getNumberofReplicas(tableName, schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#lookup(java.lang.String)
	 */
	@Override
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException {
		return inMemory.lookup(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager)
	throws RemoteException {
		inMemory.buildSchemaManagerState(otherSchemaManager);
		persisted.buildSchemaManagerState(otherSchemaManager);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState()
	throws RemoteException {
		inMemory.buildSchemaManagerState(persisted);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getConnectionInformation()
	 */
	@Override
	public Set<DatabaseURL> getConnectionInformation() throws RemoteException {
		return inMemory.getConnectionInformation();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDataManagers()
	 */
	@Override
	public Map<TableInfo, DataManagerRemote> getDataManagers()  throws RemoteException {
		return inMemory.getDataManagers();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations()  throws RemoteException {
		return inMemory.getReplicaLocations();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() {
		try {
			inMemory.removeAllTableInformation();
			persisted.removeAllTableInformation();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

}
