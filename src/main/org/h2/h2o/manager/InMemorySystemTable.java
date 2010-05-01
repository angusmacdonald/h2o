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
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;
import org.h2.h2o.util.filter.CollectionFilter;
import org.h2.h2o.util.filter.Predicate;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class InMemorySystemTable implements ISystemTable, Remote {

	private Database database;

	/**
	 * References to every Table Manager in the database system.
	 * 
	 * <p><ul><li>Key: Full table name (incl. schema name)</li>
	 * <li>Value: reference to the table's Table Manager</li>
	 * </ul>
	 */
	private Map<TableInfo, TableManagerWrapper> tableManagers;

	/**
	 * Cached references to replicas in the database system.
	 * 
	 * <p><ul><li>Key: Full table name (inc. schema name)</li>
	 * <li>Value: reference to the location of a replica for that table.</li>
	 * </ul>
	 */
	private Map<String, Set<TableInfo>> replicaLocations;

	private Map<DatabaseURL, DatabaseInstanceWrapper> databasesInSystem = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();


	/**
	 * The next valid table set number which can be assigned by the System Table.
	 */
	private int tableSetNumber = 1;

	/**
	 * Locations where the state of the System Table is replicated.
	 */
	//private Set<DatabaseInstanceRemote> systemTableState;

	/**
	 * Maintained because a nosuchobjectexception is occasionally thrown. 
	 * See http://stackoverflow.com/questions/645208/java-rmi-nosuchobjectexception-no-such-object-in-table/854097#854097.
	 */
	public static HashSet<TableManagerRemote> tableManagerReferences = new HashSet<TableManagerRemote>();



	public InMemorySystemTable(Database database) throws Exception{
		this.database = database;

		tableManagers = new HashMap<TableInfo, TableManagerWrapper>();
		replicaLocations = new HashMap<String, Set<TableInfo>>();
		//		systemTableState = new HashSet<DatabaseInstanceRemote>();
		//
		//		systemTableState.add(database.getLocalDatabaseInstance());
	}

	/******************************************************************
	 ****	Methods which involve updating the System Table's state.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "New table successfully created: " + tableDetails);


		TableInfo basicTableInfo = tableDetails.getGenericTableInfo();


		TableManagerWrapper dmw = new TableManagerWrapper(basicTableInfo, tableManager, tableDetails.getDbURL());

		if (tableManagers.containsKey(basicTableInfo)){
			return false; //this table already exists.
		}

		tableManagerReferences.add(tableManager);
		tableManagers.put(basicTableInfo, dmw);
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
	 * @see org.h2.h2o.ISystemTable#removeTable(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean removeTableInformation(TableInfo ti) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to completely drop table '" + ti.getFullTableName() + "' from the system.");


		Set<TableInfo> toRemove = new HashSet<TableInfo>();

		if (ti.getTableName() == null){
			/*
			 * Drop the entire schema.
			 */

			for (TableInfo info: tableManagers.keySet()){
				if (info.getSchemaName().equals(ti.getSchemaName())){
					toRemove.add(info);
				}
			}

			for (TableInfo key: toRemove){
				this.tableManagers.remove(key);
			}

		} else { //Just remove the single table.

			this.tableManagers.remove(ti.getGenericTableInfo());

		}

		return true;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#addConnectionInformation(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceWrapper databaseInstanceRemote)
	throws RemoteException {

		databasesInSystem.remove(databaseURL);
		databasesInSystem.put(databaseURL, databaseInstanceRemote);

		return 1;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#addReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo ti) throws RemoteException {

		Set<TableInfo> replicas = replicaLocations.get(ti.getFullTableName());

		if (replicas == null){
			replicas = new HashSet<TableInfo>();
		}

		replicas.add(ti);

		replicaLocations.put(ti.getFullTableName(), replicas);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#removeReplica(java.lang.String, org.h2.h2o.TableInfo)
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
	 ****	Methods which involve querying the System Table.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#lookup(java.lang.String)
	 */
	@Override
	public TableManagerRemote lookup(TableInfo ti) throws RemoteException {
		TableManagerWrapper dmw = tableManagers.get(ti);
		TableManagerRemote tm = null;

		if (dmw != null){
			tm = dmw.getTableManager();
		}
		/*
		 * If there is a null reference to a Table Manager we can try to reinstantiate it, but
		 * if there is no reference at all just return null for the lookup. 
		 */
		if (tm != null || !tableManagers.containsKey(ti)) {
			if (!tableManagers.containsKey(ti)){
				return null;
			}

			return tm;
		}


		/*
		 * The DM reference is null so we must look to create a new DM.
		 * XXX is it possible that a data manager is running and the SM doesn't know of it?
		 */

		if (dmw != null && this.database.getDatabaseURL().equals(dmw.getTableManagerURL())){
			/*
			 * It is okay to re-instantiate the Table Manager here.
			 */
			//TableManager dm = TableManager.createTableManagerFromPersistentStore(ti.getSchemaName(), ti.getSchemaName());
			try {
				tm = new TableManager(ti, database);
				tm.recreateReplicaManagerState();
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

			/*
			 * Make Table Manager serializable first.
			 */
			try {
				tm = (TableManagerRemote) UnicastRemoteObject.exportObject(tm, 0);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
			this.database.getChordInterface().bind(ti.getFullTableName(), tm);

		} else if (dmw != null){
			//Try to create the data manager at whereever it is meant to be. It may already be active.
			// RECREATE TABLEMANAGER <tableName>
			try {
				this.getDatabaseInstance(dmw.getTableManagerURL()).executeUpdate("RECREATE TABLEMANAGER " + ti.getFullTableName(), false);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			}
			
			dmw = tableManagers.get(ti);
			tm = dmw.getTableManager();

		} else {
			//Table Manager location is not known.
			ErrorHandling.errorNoEvent("Couldn't find the location of the table manager for table " + ti + ". This should never happen - the relevant information" +
					" should be found in persisted state.");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ti.getFullTableName() + "'s table manager has been recreated on " + dmw.getTableManagerURL() + ".");

		dmw.setTableManager(tm);
		tableManagers.put(ti, dmw);

		return tableManagers.get(ti).getTableManager();
	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException {
		return tableManagers.containsKey(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getAllTablesInSchema(java.lang.String)
	 */
	@Override
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException {

		Set<String> tableNames = new HashSet<String>();

		for (TableInfo ti: tableManagers.keySet()){
			if (ti.getSchemaName().equals(schemaName)){
				tableNames.add(ti.getFullTableName());
			}
		}

		return tableNames;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getNewTableSetNumber()
	 */
	@Override
	public int getNewTableSetNumber() throws RemoteException {
		return tableSetNumber++;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getNumberofReplicas(java.lang.String, java.lang.String)
	 */
	@Override
	public int getNumberofReplicas(String tableName, String schemaName) throws RemoteException {
		Set<TableInfo> replicas = replicaLocations.get(schemaName + "." + tableName);

		if (replicas == null) 	return 0;
		else					return replicas.size();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState(org.h2.h2o.manager.ISystemTable)
	 */
	@Override
	public void buildSystemTableState(ISystemTable otherSystemTable)
	throws RemoteException, MovedException, SQLException {

		/*
		 * Obtain references to connected machines.
		 */
		Map<DatabaseURL, DatabaseInstanceWrapper> connectedMachines = otherSystemTable.getConnectionInformation();

		databasesInSystem = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();

		//Make sure this contains remote references for each URL

		for (Entry<DatabaseURL, DatabaseInstanceWrapper> remoteDB: connectedMachines.entrySet()){
			DatabaseInstanceWrapper wrapper = remoteDB.getValue();

			DatabaseInstanceRemote dir = null;

			if (wrapper != null) wrapper.getDatabaseInstance();

			boolean active = (remoteDB.getValue() == null)? true : remoteDB.getValue().isActive();

			if (dir == null){
				if (remoteDB.getKey().equals(database.getDatabaseURL())){
					//Local machine.
					dir = database.getLocalDatabaseInstance();
				} else {
					//Look for a remote reference.
					try {
						dir = database.getRemoteInterface().getDatabaseInstanceAt(remoteDB.getKey());

						if (dir != null) active = true;
					} catch (Exception e) {
						//Couldn't find reference to this database instance.
						active = false;
					}
				}

			}

			databasesInSystem.put(remoteDB.getKey(), new DatabaseInstanceWrapper(remoteDB.getKey(), dir, active));
		}


		/*
		 * Obtain references to Table Managers, though not necessarily references to active TM proxies.
		 */
		tableManagers = otherSystemTable.getTableManagers();

		/*
		 * At this point some of the Table Manager references will be null if the Table Managers could not be found at their old location.
		 * If a reference is null, but there is a copy of the table locally then a new Table Manager can be created.
		 * If a reference is null, but there is no local copy then the table should no longer be accessible. 
		 */

		//Map<TableInfo, TableManagerRemote> newManagers = new HashMap<TableInfo, TableManagerRemote>();

		/*
		 * Obtain references to replicas.
		 */
		//replicaLocations = otherSystemTable.getReplicaLocations();


	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException {
		return databasesInSystem;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getTableManagers()
	 */
	@Override
	public Map<TableInfo, TableManagerWrapper> getTableManagers() {
		return tableManagers;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations() {
		return replicaLocations;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState()
	 */
	@Override
	public void buildSystemTableState() throws RemoteException {
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
	 * @see org.h2.h2o.manager.ISystemTable#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() throws RemoteException{
		for (TableManagerWrapper dmw : tableManagers.values()){
			try {

				TableManagerRemote dm = null;

				if (dmw != null){
					dm = dmw.getTableManager();
				}

				if (dm != null){
					dm.shutdown();

					UnicastRemoteObject.unexportObject(dm, true);
				}
			} catch (Exception e) {
			}
		}

		tableManagers.clear();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#addSystemTableDataLocation(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void addStateReplicaLocation(
			DatabaseInstanceWrapper databaseReference) throws RemoteException {

		//		if (systemTableState.size() < Replication.SCHEMA_MANAGER_REPLICATION_FACTOR){ //TODO update to allow policy on number of replicas.
		//			this.systemTableState.add(databaseReference);
		//			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O Schema Tables replicated on new successor node: " + databaseReference.getLocation().getDbLocation());
		//		} else {
		//			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Didn't add to the System Table's replication set, because there are enough replicas already (" + systemTableState.size() + ")");
		//		}

	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL)
	throws RemoteException, MovedException {
		DatabaseInstanceWrapper wrapper = databasesInSystem.get(databaseURL);
		if (wrapper == null) return null;
		return wrapper.getDatabaseInstance();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceWrapper> getDatabaseInstances()
	throws RemoteException, MovedException {
		return new HashSet<DatabaseInstanceWrapper>(databasesInSystem.values());
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#removeDatabaseInstance(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void removeConnectionInformation(
			DatabaseInstanceRemote localDatabaseInstance)
	throws RemoteException, MovedException {
		DatabaseInstanceWrapper wrapper = this.databasesInSystem.get(localDatabaseInstance.getConnectionURL());

		assert wrapper != null;

		wrapper.setActive(false);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#changeTableManagerLocation(org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	public void changeTableManagerLocation(TableManagerRemote stub, TableInfo tableInfo) {
		Object result = this.tableManagers.remove(tableInfo.getGenericTableInfo());

		if (result == null){
			ErrorHandling.errorNoEvent("There is an inconsistency in the storage of Table Managers which has caused inconsistencies in the set of managers.");
			assert false;
		}

		TableManagerWrapper dmw = new TableManagerWrapper(tableInfo, stub, tableInfo.getDbURL());

		this.tableManagers.put(tableInfo.getGenericTableInfo(), dmw);
		tableManagerReferences.add(stub);
		this.database.getChordInterface().bind(tableInfo.getFullTableName(), stub);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getLocalDatabaseInstances(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public Set<TableManagerWrapper> getLocalDatabaseInstances(DatabaseURL databaseInstance)
	throws RemoteException, MovedException {

		/*
		 * Create an interator to go through and chec whether a given Table Manager is local to the specified machine.
		 */
		Predicate<TableManagerWrapper, DatabaseURL> isLocal = new Predicate<TableManagerWrapper, DatabaseURL>() {
			public boolean apply(TableManagerWrapper wrapper, DatabaseURL databaseInstance) {
				try {
					return wrapper.isLocalTo(databaseInstance);
				} catch (RemoteException e) {
					return false;
				}
			}

		};

		Set<TableManagerWrapper> localManagers = CollectionFilter.filter(this.tableManagers.values(), isLocal, databaseInstance);

		return localManagers;
	}


}
