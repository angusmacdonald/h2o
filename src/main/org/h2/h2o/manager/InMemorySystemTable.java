/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
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
import org.h2.h2o.manager.monitorthreads.TableManagerLivenessCheckerThread;
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
	 * Where replicas for table manager state are stored in the database system.
	 * 
	 * <p><ul><li>Key: Full table name (inc. schema name)</li>
	 * <li>Value: reference to the location of a table manager state replica for that table.</li>
	 * </ul>
	 */
	private Map<TableInfo, Set<DatabaseURL>> tmReplicaLocations;

	private Map<DatabaseURL, DatabaseInstanceWrapper> databasesInSystem = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();


	/**
	 * The next valid table set number which can be assigned by the System Table.
	 */
	private int tableSetNumber = 1;

	private Map<TableInfo, DatabaseURL> primaryLocations;

	/**
	 * A thread which periodically checks that Table Managers are still alive.
	 */
	private TableManagerLivenessCheckerThread tableManagerPingerThread;
	private boolean started = false;
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
		tmReplicaLocations = new HashMap<TableInfo, Set<DatabaseURL>>();

		primaryLocations = new HashMap<TableInfo, DatabaseURL>();

		int replicationThreadSleepTime = Integer.parseInt(database.getDatabaseSettings().get("TABLE_MANAGER_LIVENESS_CHECKER_THREAD_SLEEP_TIME"));

		tableManagerPingerThread = new TableManagerLivenessCheckerThread(this, replicationThreadSleepTime);
		tableManagerPingerThread.setName("TableManagerLivenessCheckerThread");
		tableManagerPingerThread.start();

		started = true;
	}

	/******************************************************************
	 ****	Methods which involve updating the System Table's state.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails, Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "New table successfully created: " + tableDetails);

		TableInfo basicTableInfo = tableDetails.getGenericTableInfo();

		TableManagerWrapper tableManagerWrapper = new TableManagerWrapper(basicTableInfo, tableManager, tableDetails.getURL());

		if (tableManagers.containsKey(basicTableInfo)){
			return false; //this table already exists.
		}

		tableManagerReferences.add(tableManager);
		tableManagers.put(basicTableInfo, tableManagerWrapper);

		primaryLocations.put(basicTableInfo, tableDetails.getURL());

		Set<DatabaseURL> replicas = tmReplicaLocations.get(basicTableInfo);

		if (replicas == null){
			replicas = new HashSet<DatabaseURL>();
		}

		for (DatabaseInstanceWrapper wrapper: replicaLocations){
			replicas.add(wrapper.getURL());
		}


		tmReplicaLocations.put(basicTableInfo, replicas);

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
				TableManagerWrapper tmw = this.tableManagers.remove(key);

				setTableManagerAsShutdown(tmw);
			}

		} else { //Just remove the single table.

			TableManagerWrapper tmw = this.tableManagers.remove(ti.getGenericTableInfo());
			setTableManagerAsShutdown(tmw);
		}

		return true;
	}

	/**
	 * Specify that the Table Manager is no longer in use. This ensures that if any remote instances have cached references of the
	 * manager, they will become aware that it is no longer active.
	 * @param tmw
	 * @throws RemoteException
	 */
	private void setTableManagerAsShutdown(TableManagerWrapper tmw)
	throws RemoteException {
		if (tmw.getTableManager() != null){
			try {
				tmw.getTableManager().shutdown(true);
			} catch (MovedException e) {
				//This should never happen - the System Table should always know the current location.
				e.printStackTrace();
			}
		}
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

	//	/* (non-Javadoc)
	//	 * @see org.h2.h2o.ISystemTable#addReplicaInformation(org.h2.h2o.TableInfo)
	//	 */
	//	@Override
	//	public void addReplicaInformation(TableInfo ti) throws RemoteException {
	//
	//		Set<TableInfo> replicas = replicaLocations.get(ti.getFullTableName());
	//
	//		if (replicas == null){
	//			replicas = new HashSet<TableInfo>();
	//		}
	//
	//		replicas.add(ti);
	//
	//		replicaLocations.put(ti.getFullTableName(), replicas);
	//	}
	//
	//	/* (non-Javadoc)
	//	 * @see org.h2.h2o.ISystemTable#removeReplica(java.lang.String, org.h2.h2o.TableInfo)
	//	 */
	//	@Override
	//	public void removeReplicaInformation(TableInfo ti) throws RemoteException {
	//		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to drop a single replica of '" + ti.getFullTableName() + "' from the system.");
	//
	//		Set<TableInfo> replicas = replicaLocations.get(ti.getFullTableName());
	//
	//		if (replicas == null){
	//			return;
	//		}
	//
	//		replicas.remove(ti);
	//
	//	}

	/******************************************************************
	 ****	Methods which involve querying the System Table.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#lookup(java.lang.String)
	 */
	@Override
	public TableManagerWrapper lookup(TableInfo ti) throws RemoteException {
		ti = ti.getGenericTableInfo();
		TableManagerWrapper tableManagerWrapper = tableManagers.get(ti);
		TableManagerRemote tm = null;

		if (tableManagerWrapper != null){
			tm = tableManagerWrapper.getTableManager();
		}
		/*
		 * If there is a null reference to a Table Manager we can try to reinstantiate it, but
		 * if there is no reference at all just return null for the lookup. 
		 */
		boolean containsTableManager = tableManagers.containsKey(ti);
		if (tm != null || !containsTableManager) {
			if (!containsTableManager){
				return null;
			}

			return tableManagerWrapper;
		}


		/*
		 * The DM reference is null so we must look to create a new DM.
		 * XXX is it possible that a data manager is running and the SM doesn't know of it?
		 */

		if (tableManagerWrapper != null && this.database.getURL().equals(tableManagerWrapper.getURL())){
			/*
			 * It is okay to re-instantiate the Table Manager here.
			 */
			//TableManager dm = TableManager.createTableManagerFromPersistentStore(ti.getSchemaName(), ti.getSchemaName());
			try {
				tm = new TableManager(ti, database);
				tm.recreateReplicaManagerState(tableManagerWrapper.getURL().sanitizedLocation());
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

		} else if (tableManagerWrapper != null){
			//Try to create the data manager at whereever it is meant to be. It may already be active.
			// RECREATE TABLEMANAGER <tableName>
			try {
				this.getDatabaseInstance(tableManagerWrapper.getURL()).executeUpdate("RECREATE TABLEMANAGER " + ti.getFullTableName() + " FROM " + ti.getURL().sanitizedLocation(), false);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			}

			tableManagerWrapper = tableManagers.get(ti);
			tm = tableManagerWrapper.getTableManager();

		} else {
			//Table Manager location is not known.
			ErrorHandling.errorNoEvent("Couldn't find the location of the table manager for table " + ti + ". This should never happen - the relevant information" +
			" should be found in persisted state.");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ti.getFullTableName() + "'s table manager has been recreated on " + tableManagerWrapper.getURL() + ".");

		tableManagerWrapper.setTableManager(tm);
		tableManagers.put(ti, tableManagerWrapper);

		return tableManagerWrapper;
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

	//	/* (non-Javadoc)
	//	 * @see org.h2.h2o.ISystemTable#getNumberofReplicas(java.lang.String, java.lang.String)
	//	 */
	//	@Override
	//	public int getNumberofReplicas(String tableName, String schemaName) throws RemoteException {
	//		Set<TableInfo> replicas = replicaLocations.get(schemaName + "." + tableName);
	//
	//		if (replicas == null) 	return 0;
	//		else					return replicas.size();
	//	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState(org.h2.h2o.manager.ISystemTable)
	 */
	@Override
	public void buildSystemTableState(ISystemTable otherSystemTable)
	throws RemoteException, MovedException, SQLException {
		started = false;
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
				if (remoteDB.getKey().equals(database.getURL())){
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

		tmReplicaLocations = otherSystemTable.getReplicaLocations();
		primaryLocations = otherSystemTable.getPrimaryLocations();

		/*
		 * At this point some of the Table Manager references will be null if the Table Managers could not be found at their old location.
		 * 
		 * BUT, a new Table Manager cannot be created at this point because it would require contact with the System Table, which is not yet active.
		 */

		started = true;

	}


	public void removeTableManagerCheckerThread() {
		tableManagerPingerThread.setRunning(false);
	}

	/**
	 * Check that Table Managers are still alive.
	 * @return 
	 */
	public boolean checkTableManagerAccessibility(){
		boolean anyTableManagerRecreated = false;

		if (started) {

			for (TableManagerWrapper tableManagerWrapper: tableManagers.values()){
				TableManagerRemote tm = tableManagerWrapper.getTableManager();

				boolean thisTableManagerRecreated = recreateTableManagerIfNotAlive(tableManagerWrapper);

				if (thisTableManagerRecreated) anyTableManagerRecreated = true;
			}

		}
		return anyTableManagerRecreated;
	}

	/**
	 * Checks whether a table manager is currently active.
	 * @param tableManager
	 * @return
	 */
	private static boolean isAlive(TableManagerRemote tableManager) {
		boolean alive = true;

		if (tableManager == null) alive = false;
		try {
			tableManager.checkConnection();
		} catch (Exception e) {
			alive = false;
		}
		return alive;
	}

	public TableManagerRemote recreateTableManager(TableInfo tableInfo) {
		TableManagerWrapper tableManager = tableManagers.get(tableInfo);

		recreateTableManagerIfNotAlive(tableManager);

		return tableManagers.get(tableInfo).getTableManager();
	}

	public synchronized boolean recreateTableManagerIfNotAlive(TableManagerWrapper tableManagerWrapper) {

		if (isAlive(tableManagerWrapper.getTableManager())) return false; //check that it isn't already active.

		for (DatabaseURL replicaLocation: tmReplicaLocations.get(tableManagerWrapper.getTableInfo())){

			if (replicaLocation.equals(tableManagerWrapper.getURL())) continue;

			try{
				DatabaseInstanceWrapper instance = databasesInSystem.get(replicaLocation);

				boolean success = instance.getDatabaseInstance().recreateTableManager(tableManagerWrapper.getTableInfo(), tableManagerWrapper.getURL());

				if (success) {
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Table Manager for " + tableManagerWrapper.getTableInfo() + " recreated on " + instance.getURL());

					return true;
				}
			} catch (RemoteException e) {
				//May fail on some nodes.

				//TODO mark these instances as inactive.
			}
		}

		ErrorHandling.errorNoEvent("Failed to recreate Table Manager for " + tableManagerWrapper.getTableInfo() + ". There were " + tmReplicaLocations.get(tableManagerWrapper.getTableInfo()).size() + 
		" replicas available (including the failed machine).");
		return false;
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
	public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations() {
		return tmReplicaLocations;
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
	public boolean addStateReplicaLocation(
			DatabaseInstanceWrapper databaseReference) throws RemoteException {
		return true; //This class doesn't do replication. See the PersistentSystemTable class.
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
		DatabaseInstanceWrapper wrapper = this.databasesInSystem.get(localDatabaseInstance.getURL());

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

		TableManagerWrapper dmw = new TableManagerWrapper(tableInfo, stub, tableInfo.getURL());

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

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#addTableManagerStateReplica(org.h2.h2o.util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void addTableManagerStateReplica(TableInfo table, DatabaseURL replicaLocation, DatabaseURL primaryLocation, boolean active) throws RemoteException, MovedException {
		Set<DatabaseURL> replicas = tmReplicaLocations.get(table.getGenericTableInfo());

		primaryLocations.put(table.getGenericTableInfo(), primaryLocation);

		if (replicas == null){
			replicas = new HashSet<DatabaseURL>();
		}

		replicas.add(replicaLocation);

		tmReplicaLocations.put(table.getGenericTableInfo(), replicas);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#removeTableManagerStateReplica(org.h2.h2o.util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void removeTableManagerStateReplica(TableInfo table,
			DatabaseURL replicaLocation) throws RemoteException, MovedException {
		Set<DatabaseURL> replicas = tmReplicaLocations.get(table.getGenericTableInfo());

		if (replicas == null){
			throw new RemoteException("Tried to remove table manager replica state for a table which wasn't found " + table);
		}

		boolean removed = replicas.remove(replicaLocation);

		if (!removed){
			throw new RemoteException("Tried to remove table manager replica state for a replica which wasn't found " + table + " at " + replicaLocation);
		}

	}

	public Map<TableInfo, DatabaseURL> getPrimaryLocations() {
		return primaryLocations;
	}

}
