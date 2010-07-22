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
package org.h2.h2o.comms;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.manager.ISystemTable;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.PersistentSystemTable;
import org.h2.h2o.manager.TableManager;
import org.h2.h2o.util.TableInfo;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class MetaDataReplicaManager {
	/*
	 * TABLE MANAGER STATE
	 */
	/**
	 * Manages the location of Table Manager replicas.
	 */
	private ReplicaManager tableManagerReplicas;

	/*
	 * SYSTEM TABLE STATE.
	 */
	/**
	 * Managers the location of System Table replicas.
	 */
	private ReplicaManager systemTableReplicas;


	/*
	 * QUERIES
	 */
	private String addNewReplicaLocationQuery;
	private String addNewSystemTableQuery;
	private String dropOldTableManagerReplica;
	private String dropOldSystemTableReplica;

	/*
	 * CONFIGURATION OPTIONS.
	 */
	/**
	 * Whether the System is replicating meta-data at all.
	 */
	private boolean metaDataReplicationEnabled;

	/**
	 * The number of replicas required of System Table meta-data.
	 */
	private int systemTableReplicationFactor;

	/**
	 * The number of replicas required of Table Manager meta-data.
	 */
	private int tableManagerReplicationFactor;

	/*
	 * DATABASE STATE.
	 */
	private Parser parser;

	/**
	 * Location of the local database instance.
	 */
	private DatabaseInstanceWrapper localDatabase;

	private Database db;



	public MetaDataReplicaManager(boolean metaDataReplicationEnabled, int systemTableReplicationFactor, int tableManagerReplicationFactor, DatabaseInstanceWrapper localDatabase, Database db){
		/*
		 * Replica Locations
		 */
		this.tableManagerReplicas = new ReplicaManager();
		this.systemTableReplicas = new ReplicaManager();

		this.tableManagerReplicas.add(localDatabase);
		this.systemTableReplicas.add(localDatabase);

		/*
		 * Configuration options.
		 */
		this.metaDataReplicationEnabled = metaDataReplicationEnabled;
		this.systemTableReplicationFactor = systemTableReplicationFactor;
		this.tableManagerReplicationFactor = tableManagerReplicationFactor;

		/*
		 * Local Machine Details.
		 */
		this.db = db;
		this.localDatabase = localDatabase;
		this.parser = new Parser(db.getSystemSession(), true);

		/*
		 * Queries.
		 */
		String databaseName = db.getURL().sanitizedLocation().toUpperCase();

		addNewReplicaLocationQuery = "CREATE REPLICA IF NOT EXISTS " + TableManager.getMetaTableName(databaseName, TableManager.TABLES) + ", " + (TableManager.getMetaTableName(databaseName, TableManager.REPLICAS) + ", ") + 
		TableManager.getMetaTableName(databaseName, TableManager.CONNECTIONS) + " FROM '" + db.getURL().getOriginalURL() + "';";

		addNewSystemTableQuery = "CREATE REPLICA IF NOT EXISTS " + PersistentSystemTable.TABLES + ", " + 
		PersistentSystemTable.CONNECTIONS + (", " + PersistentSystemTable.TABLEMANAGERSTATE) + " FROM '" + db.getURL().getOriginalURL() + "';";


		dropOldSystemTableReplica = "DROP REPLICA IF EXISTS " + PersistentSystemTable.TABLES + ", " + 
		PersistentSystemTable.CONNECTIONS + ", " + PersistentSystemTable.TABLEMANAGERSTATE + ";";

		dropOldTableManagerReplica = "DROP REPLICA IF EXISTS " + TableManager.getMetaTableName(databaseName, TableManager.TABLES) + ", " + 
		TableManager.getMetaTableName(databaseName, TableManager.CONNECTIONS) + ", " + TableManager.getMetaTableName(databaseName, TableManager.REPLICAS) + ";";
	}


	/**
	 * Attempts to replicate local meta-data to any available machines, until the desired replication factor is reached.
	 * @param systemTableRef
	 */
	public synchronized void replicateTableManagersIfPossible(ISystemTableReference systemTableRef) {
		
		ReplicaManager replicaManager = tableManagerReplicas;
		int managerStateReplicationFactor = tableManagerReplicationFactor;


		if ( !metaDataReplicationEnabled || (replicaManager.size() >= managerStateReplicationFactor)){
			return; //replication factor already reached, or replication is not enabled.
		}

		Set<DatabaseInstanceWrapper> databaseInstances = null;

		try {
			ISystemTable systemTable = systemTableRef.getSystemTable();

			if (systemTable == null){
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "System table was NULL so the meta-data manager is unable to replicate.");
				return;
			} else {
				databaseInstances = systemTable.getDatabaseInstances();
			}
		} catch (Exception e) {
			e.printStackTrace();
			return; //just return, the system will attempt to replicate later on.
		}

		if (databaseInstances.size() == 1) return;

		for (DatabaseInstanceWrapper databaseInstance: databaseInstances){
			if (!isLocal(databaseInstance) && databaseInstance.isActive()){
				try {
					addReplicaLocation(databaseInstance, false);

					if (replicaManager.size() >= managerStateReplicationFactor) break;
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public boolean addReplicaLocation(DatabaseInstanceWrapper newReplicaLocation, boolean isSystemTable) throws RemoteException{
		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable)? systemTableReplicationFactor: tableManagerReplicationFactor;

		 Set<TableInfo> localTableManagers = db.getSystemTableReference().getLocalTableManagers().keySet();
		
		if (metaDataReplicationEnabled){
			if (replicaManager.size() < managerStateReplicationFactor){ 

				//now replica state here.
				try {
					/*
					 * Remove existing entries for this System Table / Table Manager.
					 */
					String deleteOldEntries = null;
					if (isSystemTable){
						deleteOldEntries = this.dropOldSystemTableReplica;
					} else if (localTableManagers.size() > 0){ //if there are any local Table Managers clear old meta data on them from remote machines.
						deleteOldEntries = this.dropOldTableManagerReplica; //constructRemoveReplicaQuery();
					}

					if (deleteOldEntries != null){
						newReplicaLocation.getDatabaseInstance().executeUpdate(deleteOldEntries, true);
					}

					/*
					 * Create new replica if needed, then replicate state.
					 */
					String createQuery = (isSystemTable)? addNewSystemTableQuery: addNewReplicaLocationQuery;

					newReplicaLocation.getDatabaseInstance().executeUpdate(createQuery, true);

					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O " + ((isSystemTable)? "System Table": "Table Manager") + " tables on " + localDatabase.getURL() + " replicated onto new node: " + newReplicaLocation.getURL().getDbLocation());

					replicaManager.add(newReplicaLocation);

					for (TableInfo ti: localTableManagers){
						
						db.getSystemTableReference().getSystemTable().addTableManagerStateReplica(ti, newReplicaLocation.getURL(), localDatabase.getURL(), true);
					}

					return true;
				} catch (SQLException e) {
					e.printStackTrace();
					ErrorHandling.errorNoEvent("Failed to replicate manager/table state onto: " + newReplicaLocation.getURL().getDbLocation() + ", local machine: " + localDatabase.getURL());
				} catch (Exception e) {
					throw new RemoteException(e.getMessage());
				} 

			}
		}
		return false;
	}

	public int executeUpdate(String query, boolean isSystemTable, TableInfo tableInfo) throws SQLException{

		//Loop through replicas
		//Asynchrously send update.

		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;

		Set<DatabaseInstanceWrapper> replicas = replicaManager.getActiveReplicas();
		Set<DatabaseInstanceWrapper> failed = new HashSet<DatabaseInstanceWrapper>();

		int result = -1;

		for (DatabaseInstanceWrapper replica: replicas){
			if (isLocal(replica)){
				Command sqlQuery = parser.prepareCommand(query);

				try {
					result = sqlQuery.update();

					sqlQuery.close();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			} else {
				try {
					result = replica.getDatabaseInstance().executeUpdate(query, true);
				} catch (RemoteException e) {
					failed.add(replica);

					if (!isSystemTable){
						//Remove table replica information from the system table.
						try {
							this.db.getSystemTable().removeTableManagerStateReplica(tableInfo, replica.getURL());
						} catch (RemoteException e1) {
							e1.printStackTrace();
						} catch (MovedException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}

		boolean hasRemoved = replicas.removeAll(failed);

		if (hasRemoved){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Removed one or more replica locations because they couldn't be contacted for the last update.");
		}

		return result;
	}

	/**
	 * Called when a new replica location is added by the local meta data replica manager.
	 * 
	 * 
	 * 
	 * @param tableID
	 * @param connectionID
	 * @param primaryLocationConnectionID
	 * @param active
	 * @return
	 * @throws SQLException
	 */
	public int addTableManagerReplicaInformation(int tableID, int connectionID, int primaryLocationConnectionID, boolean active) throws SQLException{
		String sql = "INSERT INTO " + PersistentSystemTable.TABLEMANAGERSTATE + " VALUES (" + tableID + ", " + connectionID + ", " + primaryLocationConnectionID + ", " + active + ");";

		return executeUpdate(sql, true, null);
	}


	/**
	 * When a table is created this must be called, and all replica locations should be added (there may be more than one
	 * replica location when the table is created).
	 * 
	 * @param tableID
	 * @param connectionID
	 * @param primaryLocationConnectionID
	 * @param active
	 * @param tableDetails
	 * @return
	 */
	public Set<DatabaseInstanceWrapper> getTableManagerReplicaLocations() {
		return this.tableManagerReplicas.getActiveReplicas();
	}



	private boolean isLocal(DatabaseInstanceWrapper replica) {
		return replica.equals(localDatabase);
	}

	public String[] getReplicaLocations(boolean isSystemTable) {

		if (isSystemTable){
			return systemTableReplicas.getReplicaLocations();
		} else {
			return tableManagerReplicas.getReplicaLocations();
		}
	}

	public void remove(DatabaseInstanceRemote databaseInstance, boolean isSystemTable) {
		ReplicaManager replicaManager = (isSystemTable)? systemTableReplicas: tableManagerReplicas;

		replicaManager.remove(databaseInstance);
	}

	/**
	 * Called when a new Table Manager replica lcoation is added, or a new Table Manager is added.
	 * 
	 * This deletes any outdated information held by remote machines on local Table Managers.
	 * @return
	 * @throws SQLException
	 */
	public String constructRemoveReplicaQuery() throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();
		String replicaRelation =  TableManager.getMetaTableName(databaseName, TableManager.REPLICAS);
		String tableRelation =  TableManager.getMetaTableName(databaseName, TableManager.TABLES);

		String deleteReplica = "DELETE FROM " + tableRelation + " WHERE ";
		String deleteTable = "DELETE FROM " + replicaRelation + " WHERE ";

		boolean includeAnd = false;
		
		Set<TableInfo> localTableManagers = db.getSystemTableReference().getLocalTableManagers().keySet();
		
		for (TableInfo ti: localTableManagers){

			int tableID = getTableID(ti, false);

			if (includeAnd){
				deleteReplica += " AND ";
				deleteTable += " AND ";
			} else {
				includeAnd = true;
			}

			deleteReplica += "table_id=" + tableID + " ";

			deleteTable += "table_id=" + tableID + " ";
		}

		deleteReplica += ";";
		deleteTable += ";";

		return deleteReplica + deleteTable;
	}

	public String constructRemoveReplicaQuery(TableInfo ti, boolean removeReplicaInfo, boolean isSystemTable) throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();
		String replicaRelation =  (isSystemTable)? null: TableManager.getMetaTableName(databaseName, TableManager.REPLICAS);
		String tableRelation = (isSystemTable)? PersistentSystemTable.TABLES: TableManager.getMetaTableName(databaseName, TableManager.TABLES);

		String sql = "";
		if (ti.getTableName() == null){
			/*
			 * Deleting the entire schema.
			 */
			Integer[] tableIDs;

			tableIDs = getTableIDs(ti.getSchemaName(), isSystemTable);


			if (tableIDs.length > 0 && removeReplicaInfo){
				sql = "DELETE FROM " + replicaRelation;
				for (int i = 0; i < tableIDs.length; i++){
					if (i==0){
						sql +=" WHERE table_id=" + tableIDs[i];
					} else {
						sql +=" OR table_id=" + tableIDs[i];
					}
				}
				sql += ";";
			} else {
				sql = "";
			}

			sql += "\nDELETE FROM " + tableRelation + " WHERE schemaname='" + ti.getSchemaName() + "'; ";
		} else {

			int tableID = getTableID(ti, isSystemTable);

			sql = "";

			if (removeReplicaInfo){
				sql = "DELETE FROM " + replicaRelation + " WHERE table_id=" + tableID + ";";
			}
			sql += "\nDELETE FROM " + tableRelation + " WHERE table_id=" + tableID + "; ";

		}
		return sql;
	}

	/**
	 *  Gets the table ID for a given database table if it is present in the database. If not, an exception is thrown.
	 * @param tableName		Name of the table
	 * @param schemaName	Schema the table is in.
	 * @return
	 */
	public int getTableID(TableInfo ti, boolean isSystemTable) throws SQLException{
		String databaseName = localDatabase.getURL().sanitizedLocation();

		String tableRelation = (isSystemTable)? PersistentSystemTable.TABLES: TableManager.getMetaTableName(databaseName, TableManager.TABLES);

		String sql = "SELECT table_id FROM " + tableRelation + " WHERE tablename='" + ti.getTableName()
		+ "' AND schemaname='" + ti.getSchemaName() + "';";

		LocalResult result = null;

		Command sqlQuery = parser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()){
			return result.currentRow()[0].getInt();
		} else {
			throw new SQLException("Internal problem: tableID not found in System Table.");
		}
	}


	/**
	 * Gets all the table IDs for tables in a schema.
	 * @param schemaName
	 * @return
	 * @throws SQLException 
	 */
	protected Integer[] getTableIDs(String schemaName, boolean isSystemTable) throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();

		String tableRelation = (isSystemTable)? PersistentSystemTable.TABLES: TableManager.getMetaTableName(databaseName, TableManager.TABLES);


		String sql = "SELECT table_id FROM " + tableRelation + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		Command sqlQuery = parser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(0);

		Set<Integer> ids = new HashSet<Integer>();
		while (result.next()){
			ids.add(result.currentRow()[0].getInt());
		}

		return ids.toArray(new Integer[0]);
	}

}
