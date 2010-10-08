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
package org.h2o.db.replication;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.result.LocalResult;
import org.h2o.autonomic.decision.ranker.metric.CreateReplicaRequest;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.manager.TableManager;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.client.DatabaseStates;
import org.h2o.viewer.client.H2OEvent;
import org.h2o.viewer.client.H2OEventBus;

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

	public MetaDataReplicaManager(boolean metaDataReplicationEnabled, int systemTableReplicationFactor, int tableManagerReplicationFactor,
			DatabaseInstanceWrapper localDatabase, Database db) {
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

		addNewReplicaLocationQuery = "CREATE REPLICA IF NOT EXISTS " + TableManager.getMetaTableName(databaseName, TableManager.TABLES)
				+ ", " + (TableManager.getMetaTableName(databaseName, TableManager.REPLICAS) + ", ")
				+ TableManager.getMetaTableName(databaseName, TableManager.CONNECTIONS) + " FROM '" + db.getURL().getOriginalURL() + "';";

		addNewSystemTableQuery = "CREATE REPLICA IF NOT EXISTS " + PersistentSystemTable.TABLES + ", " + PersistentSystemTable.CONNECTIONS
				+ (", " + PersistentSystemTable.TABLEMANAGERSTATE) + " FROM '" + db.getURL().getOriginalURL() + "';";

		dropOldSystemTableReplica = "DROP REPLICA IF EXISTS " + PersistentSystemTable.TABLES + ", " + PersistentSystemTable.CONNECTIONS
				+ ", " + PersistentSystemTable.TABLEMANAGERSTATE + ";";

		dropOldTableManagerReplica = "DROP REPLICA IF EXISTS " + TableManager.getMetaTableName(databaseName, TableManager.TABLES) + ", "
				+ TableManager.getMetaTableName(databaseName, TableManager.CONNECTIONS) + ", "
				+ TableManager.getMetaTableName(databaseName, TableManager.REPLICAS) + ";";
	}

	/**
	 * Attempts to replicate local meta-data to the machine provided by the parameter.
	 */
	public synchronized void replicateMetaDataIfPossible(ISystemTableReference systemTableRef, boolean isSystemTable,
			DatabaseInstanceWrapper successorInstance) {
		if (isSystemTable && !systemTableRef.isSystemTableLocal())
			return;

		ReplicaManager replicaManager = (isSystemTable) ? systemTableReplicas : tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable) ? systemTableReplicationFactor : tableManagerReplicationFactor;

		if (!metaDataReplicationEnabled || (replicaManager.allReplicasSize() >= managerStateReplicationFactor)) {
			return; // replication factor already reached, or replication is not
					// enabled.
		}

		if (!isLocal(successorInstance) && successorInstance.isActive()) {
			try {
				addReplicaLocation(successorInstance, isSystemTable);
			} catch (RemoteException e) {
				// May fail.
			}
		}
	}

	/**
	 * Attempts to replicate local meta-data to any available machines, until the desired replication factor is reached.
	 * 
	 * @param systemTableRef
	 */
	public synchronized void replicateMetaDataIfPossible(ISystemTableReference systemTableRef, boolean isSystemTable) {
		if (isSystemTable && !systemTableRef.isSystemTableLocal())
			return;

		ReplicaManager replicaManager = (isSystemTable) ? systemTableReplicas : tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable) ? systemTableReplicationFactor : tableManagerReplicationFactor;

		if (!metaDataReplicationEnabled || (replicaManager.allReplicasSize() >= managerStateReplicationFactor)) {
			return; // replication factor already reached, or replication is not
					// enabled.
		}

		Queue<DatabaseInstanceWrapper> databaseInstances = null;

		try {
			ISystemTable systemTable = systemTableRef.getSystemTable();

			if (systemTable == null) {
				Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "System table was NULL so the meta-data manager is unable to replicate.");
				return;
			} else {
				databaseInstances = systemTable.getAvailableMachines(new CreateReplicaRequest(20, 100, 200));
			}
		} catch (Exception e) {
			e.printStackTrace();
			return; // just return, the system will attempt to replicate later on.
		}

		if (databaseInstances.size() == 1)
			return;

		for (DatabaseInstanceWrapper databaseInstance : databaseInstances) {

			if (!isLocal(databaseInstance) && databaseInstance.isActive()) {
				try {

					addReplicaLocation(databaseInstance, isSystemTable);

					if (replicaManager.allReplicasSize() >= managerStateReplicationFactor)
						break;
				} catch (RemoteException e) {
					// May fail. Try next database.
				}
			}
		}
	}

	private boolean addReplicaLocation(DatabaseInstanceWrapper newReplicaLocation, boolean isSystemTable) throws RemoteException {
		if (newReplicaLocation.getURL().equals(db.getURL()))
			return false; // can't replicate to the local machine

		return addReplicaLocation(newReplicaLocation, isSystemTable, 0);
	}

	private boolean addReplicaLocation(DatabaseInstanceWrapper newReplicaLocation, boolean isSystemTable, int numberOfPreviousAttempts)
			throws RemoteException {
		ReplicaManager replicaManager = (isSystemTable) ? systemTableReplicas : tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable) ? systemTableReplicationFactor : tableManagerReplicationFactor;

		Set<TableInfo> localTableManagers = db.getSystemTableReference().getLocalTableManagers().keySet();

		if (metaDataReplicationEnabled) {
			if (replicaManager.allReplicasSize() < managerStateReplicationFactor) {

				// now replica state here.
				try {
					/*
					 * Remove existing entries for this System Table / Table Manager.
					 */
					String deleteOldEntries = null;
					if (isSystemTable) {
						deleteOldEntries = this.dropOldSystemTableReplica;
					} else if (localTableManagers.size() > 0) { // if there are any local Table Managers clear old meta data on them from
																// remote machines.
						deleteOldEntries = this.dropOldTableManagerReplica;
					}

					if (deleteOldEntries != null) {
						newReplicaLocation.getDatabaseInstance().executeUpdate(deleteOldEntries, true);
					}

					/*
					 * Create new replica if needed, then replicate state.
					 */
					String createQuery = (isSystemTable) ? addNewSystemTableQuery : addNewReplicaLocationQuery;
					
					newReplicaLocation.getDatabaseInstance().executeUpdate(createQuery, true);

					Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "H2O " + ((isSystemTable) ? "System Table" : "Table Manager")
							+ " tables on " + localDatabase.getURL() + " replicated onto new node: "
							+ newReplicaLocation.getURL().getDbLocation());

					
					H2OEventBus.publish(new H2OEvent(db.getURL().getDbLocation(), DatabaseStates.META_TABLE_REPLICA_CREATION, newReplicaLocation.getURL()
							.getURL()));

					replicaManager.add(newReplicaLocation);

					for (TableInfo ti : localTableManagers) {
						db.getSystemTableReference().getSystemTable()
								.addTableManagerStateReplica(ti, newReplicaLocation.getURL(), localDatabase.getURL(), true);
					}

					if (isSystemTable) {
						try {
							updateLocatorFiles(isSystemTable);
						} catch (Exception e) {
							throw new RemoteException(e.getMessage());
						}
					}

					return true;
				} catch (SQLException e) {
					//e.printStackTrace();
					/*
					 * Usually thrown if the CREATE REPLICA command couldn't connect back to this database. This often happens when the
					 * database has only recently started and hasn't fully initialized, so this code attempts the operation again.
					 */
					if (numberOfPreviousAttempts < 5) {
						try {
							Thread.sleep(10);
						} catch (InterruptedException e1) {
						}
						return addReplicaLocation(newReplicaLocation, isSystemTable, numberOfPreviousAttempts + 1);
					} else {
						ErrorHandling.errorNoEvent(localDatabase.getURL() + " failed to replicate "
								+ ((isSystemTable) ? "System Table" : "Table Manager") + " state onto: "
								+ newReplicaLocation.getURL().getDbLocation() + ".");
					}
				} catch (Exception e) {
					/*
					 * Usually thrown if this database couldn't connect to the remote instance.
					 */
					throw new RemoteException(e.getMessage());
				}

			}
		}
		return false;
	}

	public synchronized int executeUpdate(String query, boolean isSystemTable, TableInfo tableInfo) throws SQLException {

		// Loop through replicas
		// TODO Parallelise sending of update.

		ReplicaManager replicaManager = (isSystemTable) ? systemTableReplicas : tableManagerReplicas;
		int managerStateReplicationFactor = (isSystemTable) ? systemTableReplicationFactor : tableManagerReplicationFactor;

		Map<DatabaseInstanceWrapper, Integer> replicas = replicaManager.getActiveReplicas();
		Map<DatabaseInstanceWrapper, Integer> failed = new HashMap<DatabaseInstanceWrapper, Integer>();

		int result = -1;
		for (DatabaseInstanceWrapper replica : replicas.keySet()) {

			if (isLocal(replica)) {
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
					failed.put(replica, replicas.get(replica));
				}
			}
		}

		boolean hasRemoved = failed.size() > 0;

		if (hasRemoved) {
			if (!isSystemTable) {
				// Remove table replica information from the system table.
				for (DatabaseInstanceWrapper replica : failed.keySet()) {
					try {
						this.db.getSystemTable().removeTableManagerStateReplica(tableInfo, replica.getURL());
					} catch (RemoteException e1) {
						e1.printStackTrace();
					} catch (MovedException e1) {
						e1.printStackTrace();
					}
				}
			}
			Diagnostic.traceNoEvent(DiagnosticLevel.INIT,
					"Removed one or more replica locations because they couldn't be contacted for the last update.");
		}

		replicaManager.remove(failed.keySet());

		// Check that there is a sufficient replication factor.
		if (!isSystemTable && metaDataReplicationEnabled && (replicas.size() < managerStateReplicationFactor)) {
			Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Insufficient replication factor (" + replicas.size() + "<"
					+ managerStateReplicationFactor + ") of Table Manager State on " + db.getURL());
			replicateMetaDataIfPossible(db.getSystemTableReference(), isSystemTable);
		}

		return result;
	}

	/**
	 * 
	 */
	public void updateLocatorFiles(boolean isSystemTable) throws Exception {
		LocalH2OProperties persistedInstanceInformation = new LocalH2OProperties(db.getURL());
		persistedInstanceInformation.loadProperties();

		String descriptorLocation = persistedInstanceInformation.getProperty("descriptor");
		String databaseName = persistedInstanceInformation.getProperty("databaseName");

		if (descriptorLocation == null || databaseName == null) {
			throw new Exception(
					"The location of the database descriptor must be specifed (it was not found). The database will now terminate.");
		}
		H2OLocatorInterface dl = new H2OLocatorInterface(databaseName, descriptorLocation);

		boolean successful = dl.setLocations(getReplicaLocations(isSystemTable));

		if (!successful) {
			ErrorHandling.errorNoEvent("Didn't successfully write new System Replica locations to a majority of locator servers.");
		}
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
	public int addTableManagerReplicaInformation(int tableID, int connectionID, int primaryLocationConnectionID, boolean active)
			throws SQLException {
		String sql = "INSERT INTO " + PersistentSystemTable.TABLEMANAGERSTATE + " VALUES (" + tableID + ", " + connectionID + ", "
				+ primaryLocationConnectionID + ", " + active + ");";

		return executeUpdate(sql, true, null);
	}

	/**
	 * When a table is created this must be called, and all replica locations should be added (there may be more than one replica location
	 * when the table is created).
	 * 
	 * @param tableID
	 * @param connectionID
	 * @param primaryLocationConnectionID
	 * @param active
	 * @param tableDetails
	 * @return
	 */
	public Set<DatabaseInstanceWrapper> getTableManagerReplicaLocations() {
		return new HashSet<DatabaseInstanceWrapper>(this.tableManagerReplicas.getActiveReplicas().keySet());
	}

	private boolean isLocal(DatabaseInstanceWrapper replica) {
		return replica.equals(localDatabase);
	}

	public String[] getReplicaLocations(boolean isSystemTable) {

		if (isSystemTable) {
			return systemTableReplicas.getReplicaLocationsAsStrings();
		} else {
			return tableManagerReplicas.getReplicaLocationsAsStrings();
		}
	}

	public void remove(DatabaseInstanceRemote databaseInstance, boolean isSystemTable) {
		ReplicaManager replicaManager = (isSystemTable) ? systemTableReplicas : tableManagerReplicas;

		replicaManager.remove(databaseInstance);
	}

	/**
	 * Called when a new Table Manager replica location is added, or a new Table Manager is added.
	 * 
	 * This deletes any outdated information held by remote machines on local Table Managers.
	 * 
	 * @return
	 * @throws SQLException
	 */
	public String constructRemoveReplicaQuery() throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();
		String replicaRelation = TableManager.getMetaTableName(databaseName, TableManager.REPLICAS);
		String tableRelation = TableManager.getMetaTableName(databaseName, TableManager.TABLES);

		String deleteReplica = "DELETE FROM " + tableRelation + " WHERE ";
		String deleteTable = "DELETE FROM " + replicaRelation + " WHERE ";

		boolean includeAnd = false;

		Set<TableInfo> localTableManagers = db.getSystemTableReference().getLocalTableManagers().keySet();

		for (TableInfo ti : localTableManagers) {

			int tableID = getTableID(ti, false);

			if (includeAnd) {
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
		String replicaRelation = (isSystemTable) ? null : TableManager.getMetaTableName(databaseName, TableManager.REPLICAS);
		String tableRelation = (isSystemTable) ? PersistentSystemTable.TABLES : TableManager.getMetaTableName(databaseName,
				TableManager.TABLES);

		String sql = "";

		if (ti == null || (ti.getTableName() == null && ti.getSchemaName() == null)) {
			/*
			 * Deleting everything
			 */

			if (removeReplicaInfo)
				sql = "DELETE FROM " + replicaRelation + ";";

			sql += "\nDELETE FROM " + tableRelation + ";";
		} else if (ti.getTableName() == null) {
			/*
			 * Deleting the entire schema.
			 */
			Integer[] tableIDs;

			tableIDs = getTableIDs(ti.getSchemaName(), isSystemTable);

			if (tableIDs.length > 0 && removeReplicaInfo) {
				sql = "DELETE FROM " + replicaRelation;
				for (int i = 0; i < tableIDs.length; i++) {
					if (i == 0) {
						sql += " WHERE table_id=" + tableIDs[i];
					} else {
						sql += " OR table_id=" + tableIDs[i];
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

			if (removeReplicaInfo) {
				sql = "DELETE FROM " + replicaRelation + " WHERE table_id=" + tableID + ";";
			}
			sql += "\nDELETE FROM " + tableRelation + " WHERE table_id=" + tableID + "; ";

		}
		return sql;
	}

	/**
	 * Gets the table ID for a given database table if it is present in the database. If not, an exception is thrown.
	 * 
	 * @param tableName
	 *            Name of the table
	 * @param schemaName
	 *            Schema the table is in.
	 * @return
	 */
	public int getTableID(TableInfo ti, boolean isSystemTable) throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();

		String tableRelation = (isSystemTable) ? PersistentSystemTable.TABLES : TableManager.getMetaTableName(databaseName,
				TableManager.TABLES);

		String sql = "SELECT table_id FROM " + tableRelation + " WHERE tablename='" + ti.getTableName() + "' AND schemaname='"
				+ ti.getSchemaName() + "';";

		LocalResult result = null;

		Command sqlQuery = parser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()) {
			return result.currentRow()[0].getInt();
		} else {
			throw new SQLException("Internal problem: tableID not found in System Table.");
		}
	}

	/**
	 * Gets all the table IDs for tables in a schema.
	 * 
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 */
	protected Integer[] getTableIDs(String schemaName, boolean isSystemTable) throws SQLException {
		String databaseName = localDatabase.getURL().sanitizedLocation();

		String tableRelation = (isSystemTable) ? PersistentSystemTable.TABLES : TableManager.getMetaTableName(databaseName,
				TableManager.TABLES);

		String sql = "SELECT table_id FROM " + tableRelation + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		Command sqlQuery = parser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(0);

		Set<Integer> ids = new HashSet<Integer>();
		while (result.next()) {
			ids.add(result.currentRow()[0].getInt());
		}

		return ids.toArray(new Integer[0]);
	}

}
