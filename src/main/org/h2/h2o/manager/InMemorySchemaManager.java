package org.h2.h2o.manager;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.h2o.autonomic.Replication;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class InMemorySchemaManager implements ISchemaManager, Remote {

	private Database database;

	/**
	 * References to every data manager in the database system.
	 * 
	 * <p><ul><li>Key: Full table name (incl. schema name)</li>
	 * <li>Value: reference to the table's data manager</li>
	 * </ul>
	 */
	private Map<TableInfo, DataManagerRemote> dataManagers;

	
	/**
	 * Cached references to replicas in the database system.
	 * 
	 * <p><ul><li>Key: Full table name (inc. schema name)</li>
	 * <li>Value: reference to the location of a replica for that table.</li>
	 * </ul>
	 */
	private Map<String, Set<TableInfo>> replicaLocations;

	private Map<DatabaseURL, DatabaseInstanceRemote> databasesInSystem = new HashMap<DatabaseURL, DatabaseInstanceRemote>();

	/**
	 * The next valid table set number which can be assigned by the schema manager.
	 */
	private int tableSetNumber = 1;

	/**
	 * Locations where the state of the schema manager is replicated.
	 */
	private Set<DatabaseInstanceRemote> schemaManagerState;

	public InMemorySchemaManager(Database database) throws Exception{
		this.database = database;

		dataManagers = new HashMap<TableInfo, DataManagerRemote>();
		replicaLocations = new HashMap<String, Set<TableInfo>>();

		schemaManagerState = new HashSet<DatabaseInstanceRemote>();
		
		schemaManagerState.add(database.getLocalDatabaseInstance());
	}

	/******************************************************************
	 ****	Methods which involve updating the schema manager's state.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.DataManagerRemote)
	 */
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "New table successfully created: " + tableDetails);


		TableInfo basicTableInfo = tableDetails.getGenericTableInfo();

		if (dataManagers.containsKey(basicTableInfo)){
			return false; //this table already exists.
		}

		dataManagers.put(basicTableInfo, dataManager);

		String fullName = tableDetails.getFullTableName();
		Set<TableInfo> replicas = replicaLocations.get(fullName);

		if (replicas == null){
			replicas = new HashSet<TableInfo>();
		}

		replicas.add(tableDetails);

		replicaLocations.put(tableDetails.getFullTableName(), replicas);

		return true;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeTable(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean removeTableInformation(TableInfo ti) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to completely drop table '" + ti.getFullTableName() + "' from the system.");


		Set<TableInfo> toRemove = new HashSet<TableInfo>();

		if (ti.getTableName() == null){
			/*
			 * Drop the entire schema.
			 */
			
			for (Entry<TableInfo, DataManagerRemote> dm: dataManagers.entrySet()){
				if (dm.getKey().getSchemaName().equals(ti.getSchemaName())){
					toRemove.add(dm.getKey());
				}
			}

			for (TableInfo key: toRemove){
				this.dataManagers.remove(key);
			}

		} else { //Just remove the single table.

			this.dataManagers.remove(ti.getGenericTableInfo());

		}

		return true;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addConnectionInformation(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceRemote databaseInstanceRemote)
	throws RemoteException {

		databasesInSystem.put(databaseURL, databaseInstanceRemote);

		return 1;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo ti) throws RemoteException {

		Set<TableInfo> replicas = replicaLocations.get(ti.getFullTableName());

		if (replicas == null){
			replicas = new HashSet<TableInfo>();
		}

		replicas.add(ti);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeReplica(java.lang.String, org.h2.h2o.TableInfo)
	 */
	@Override
	public void removeReplicaInformation(TableInfo ti) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to drop a single replica of '" + ti.getFullTableName() + "' from the system.");

		Set<TableInfo> replicas = replicaLocations.get(ti.getFullTableName());

		if (replicas == null){
			return;
		}

		replicas.remove(ti);

	}

	/******************************************************************
	 ****	Methods which involve querying the schema manager.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#lookup(java.lang.String)
	 */
	@Override
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException {
		DataManagerRemote dm = dataManagers.get(ti);

		/*
		 * If there is a null reference to a data manager we can try to reinstantiate it, but
		 * if there is no reference at all just return null for the lookup. 
		 */
		if (dm != null || !dataManagers.containsKey(ti)) {
			if (!dataManagers.containsKey(ti)){
				return null;
			}
			return dm;
		}


		/*
		 * The DM reference is null so we must look to create a new DM.
		 * TODO in future we may do a chord lookup to look for another dm, in a similar way to schema manager lookups.
		 */

		boolean tableExists = false;

		try {
			tableExists = countCheck("SELECT count(*) FROM information_schema.tables WHERE table_schema = '" + 
					ti.getSchemaName().toUpperCase() + "' AND table_name = '" + ti.getTableName().toUpperCase() + "';");
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (tableExists){
			/*
			 * It is okay to re-instantiate the data manager here.
			 */
			//DataManager dm = DataManager.createDataManagerFromPersistentStore(ti.getSchemaName(), ti.getSchemaName());
			try {
				dm = new DataManager(ti.getTableName(), ti.getSchemaName(), 10l, 0, database);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			/*
			 * Make data manager serializable first.
			 */
			try {
				dm = (DataManagerRemote) UnicastRemoteObject.exportObject(dm, 0);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			

		}

		dataManagers.put(ti, dm);

		return dataManagers.get(ti);
	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException {
		return dataManagers.containsKey(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getAllTablesInSchema(java.lang.String)
	 */
	@Override
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException {

		Set<String> tableNames = new HashSet<String>();

		for (TableInfo ti: dataManagers.keySet()){
			if (ti.getSchemaName().equals(schemaName)){
				tableNames.add(ti.getFullTableName());
			}
		}

		return tableNames;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNewTableSetNumber()
	 */
	@Override
	public int getNewTableSetNumber() throws RemoteException {
		return tableSetNumber++;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNumberofReplicas(java.lang.String, java.lang.String)
	 */
	@Override
	public int getNumberofReplicas(String tableName, String schemaName) throws RemoteException {
		Set<TableInfo> replicas = replicaLocations.get(schemaName + "." + tableName);

		if (replicas == null) 	return 0;
		else					return replicas.size();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager)
	throws RemoteException, MovedException {

		/*
		 * Obtain references to connected machines.
		 */
		databasesInSystem = otherSchemaManager.getConnectionInformation();

		/*
		 * Obtain references to data managers.
		 */
		dataManagers = otherSchemaManager.getDataManagers();
		
		/*
		 * At this point some of the data manager references will be null if the data managers could not be found at their old location.
		 * If a reference is null, but there is a copy of the table locally then a new data manager can be created.
		 * If a reference is null, but there is no local copy then the table should no longer be accessible. 
		 */

		//Map<TableInfo, DataManagerRemote> newManagers = new HashMap<TableInfo, DataManagerRemote>();

		/*
		 * Obtain references to replicas.
		 */
		//replicaLocations = otherSchemaManager.getReplicaLocations();


	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceRemote> getConnectionInformation() throws RemoteException {
		return databasesInSystem;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDataManagers()
	 */
	@Override
	public Map<TableInfo, DataManagerRemote> getDataManagers() {
		return dataManagers;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations() {
		return replicaLocations;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState()
	 */
	@Override
	public void buildSchemaManagerState() throws RemoteException {
		// TODO Auto-generated method stub

	}


	/**
	 * Takes in an SQL count(*) query, which should return a single result, which is a single integer, indicating
	 * the number of occurences of a given entry. If the number of entries is greater than zero true is returned; otherwise false.
	 * @param query	SQL count query.
	 * @return
	 * @throws SQLException
	 */
	private boolean countCheck(String query) throws SQLException{
		Parser queryParser = new Parser(database.getSystemSession(), true);

		Command sqlQuery = queryParser.prepareCommand(query);

		LocalResult result = sqlQuery.executeQueryLocal(0);
		if (result.next()){
			int count = result.currentRow()[0].getInt();

			return (count>0);
		}
		return false;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() throws RemoteException{
		for (DataManagerRemote dm : dataManagers.values()){
			try {

				if (dm != null){
					dm.shutdown();

					UnicastRemoteObject.unexportObject(dm, true);
				}
			} catch (Exception e) {
			}
		}

		dataManagers.clear();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#addSchemaManagerDataLocation(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void addSchemaManagerDataLocation(
			DatabaseInstanceRemote databaseReference) throws RemoteException {
		
		if (schemaManagerState.size() < Replication.SCHEMA_MANAGER_REPLICATION_FACTOR){ //TODO update to allow policy on number of replicas.
			this.schemaManagerState.add(databaseReference);
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O Schema Tables replicated on new successor node: " + databaseReference.getLocation().getDbLocation());
		} else {
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Didn't add to the schema manager's replication set, because there are enough replicas already (" + schemaManagerState.size() + ")");
		}
		
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL)
			throws RemoteException, MovedException {
		return databasesInSystem.get(databaseURL);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceRemote> getDatabaseInstances()
			throws RemoteException, MovedException {
		return new HashSet<DatabaseInstanceRemote>(databasesInSystem.values());
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeDatabaseInstance(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void removeConnectionInformation(
			DatabaseInstanceRemote localDatabaseInstance)
			throws RemoteException, MovedException {
	//	databaseInstances.remove(localDatabaseInstance.getConnectionString());
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#changeDataManagerLocation(org.h2.h2o.comms.remote.DataManagerRemote)
	 */
	public void changeDataManagerLocation(DataManagerRemote stub, TableInfo tableInfo) {
		Object result = this.dataManagers.remove(tableInfo);
		
		assert result != null;
		
		this.dataManagers.put(tableInfo, stub);
	}
}
