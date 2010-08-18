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
package org.h2o.db.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.result.LocalResult;
import org.h2.value.Value;
import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.remote.IDatabaseRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;
import org.h2o.util.exceptions.StartupException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PersistentSystemTable extends PersistentManager implements
		ISystemTable {

	/**
	 * Name of the schema used to store System Table tables.
	 */
	private static final String SCHEMA = "H2O.";

	/**
	 * Name of tables' table in System Table.
	 */
	public static final String TABLES = SCHEMA + "H2O_TABLE";

	/**
	 * Name of connections' table in System Table.
	 */
	public static final String CONNECTIONS = SCHEMA + "H2O_CONNECTION";

	/**
	 * Name of the table which stores the location of table manager state
	 * replicas.
	 */
	public static final String TABLEMANAGERSTATE = SCHEMA
			+ "H2O_TABLEMANAGER_STATE";;

	/**
	 * The database username used to communicate with System Table tables.
	 */
	public static final String USERNAME = "sa";

	/**
	 * The database password used to communicate with System Table tables.
	 */
	public static final String PASSWORD = "";

	public PersistentSystemTable(Database db, boolean createTables)
			throws Exception {
		super(db, TABLES, null, CONNECTIONS, TABLEMANAGERSTATE, true);

		if (createTables) {
			/*
			 * Create a new set of schema tables locally.
			 */
			try {
				String sql = createSQL(TABLES, CONNECTIONS);
				sql += "\n\nCREATE TABLE IF NOT EXISTS " + TABLEMANAGERSTATE
						+ "(" + "table_id INTEGER NOT NULL, "
						+ "connection_id INTEGER NOT NULL, "
						+ "primary_location_connection_id INTEGER NOT NULL, "
						+ "active BOOLEAN, "
						+ "FOREIGN KEY (table_id) REFERENCES " + TABLES
						+ " (table_id) ON DELETE CASCADE , "
						+ " FOREIGN KEY (connection_id) REFERENCES "
						+ CONNECTIONS + " (connection_id)); ";

				boolean success = getNewQueryParser();

				if (!success) {
					throw new StartupException(
							"Database has already been shutdown.");
				}
				sqlQuery = queryParser.prepareCommand(sql);

				sqlQuery.update();
				sqlQuery.close();

			} catch (SQLException e) {
				e.printStackTrace();
				throw new Exception("Couldn't create manager state tables.");
			}
		}

		super.updateLocatorFiles(true);
	}

	// public DatabaseURL getTableManagerLocation(String tableName, String
	// schemaName) throws SQLException{
	// String sql =
	// "SELECT db_location, connection_type, machine_name, connection_port " +
	// "FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
	// "WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName +
	// "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " +
	// "AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";
	//
	// LocalResult result = null;
	//
	// sqlQuery = getParser().prepareCommand(sql);
	//
	// try {
	//
	// result = sqlQuery.executeQueryLocal(1);
	//
	// } catch (Exception e){
	// e.printStackTrace();
	// }
	//
	// if (result != null && result.next()){
	// Value[] row = result.currentRow();
	//
	// String dbLocation = row[0].getString();
	// String connectionType = row[1].getString();
	// String machineName = row[2].getString();
	// String connectionPort = row[3].getString();
	//
	// DatabaseURL dbURL = new DatabaseURL(connectionType, machineName,
	// Integer.parseInt(connectionPort), dbLocation, false);
	//
	// return dbURL;
	// }
	//
	// Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Looking for table: " +
	// schemaName + "." + tableName + " (but it wasn't found).");
	// throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1,
	// tableName);
	//
	//
	// }

	// /**
	// * Creates the set of tables used by the System Table.
	// * @return Result of the update.
	// * @throws SQLException
	// */
	// protected int setupManagerStateTables() throws SQLException{
	//
	// String sql = super.createSQL(TABLES, REPLICAS, CONNECTIONS);
	//
	// String tableManagerLocation = "H2O.DATA_MANAGER_LOCATIONS";
	//
	// sql += "\n\nCREATE TABLE IF NOT EXISTS " + tableManagerLocation + "(" +
	// "replica_id INTEGER NOT NULL auto_increment(1,1), " +
	// "table_id INTEGER NOT NULL, " +
	// "connection_id INTEGER NOT NULL, " +
	// "storage_type VARCHAR(255), " +
	// "last_modification INT NOT NULL, " +
	// "table_set INT NOT NULL, " +
	// "primary_copy BOOLEAN, " +
	// "PRIMARY KEY (replica_id), " +
	// "FOREIGN KEY (table_id) REFERENCES " + tables +
	// " (table_id) ON DELETE CASCADE , " +
	// " FOREIGN KEY (connection_id) REFERENCES " + connections +
	// " (connection_id));";
	//
	// return executeUpdate(sql);
	// }

	// /**
	// * Creates linked tables for a remote System Table, location specified by
	// the paramter.
	// * @param systemTableLocation Location of the remote System Table.
	// * @return Result of the update.
	// * @throws SQLException
	// */
	// public int createLinkedTablesForSystemTable(String systemTableLocation)
	// throws SQLException{
	// String sql = "CREATE SCHEMA IF NOT EXISTS H2O;";
	// String tableName = TABLES;
	// sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName +
	// "('org.h2.Driver', '" + systemTableLocation + "', '" + USERNAME + "', '"
	// + PASSWORD + "', '" + tableName + "');";
	// tableName = CONNECTIONS;
	// sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName +
	// "('org.h2.Driver', '" + systemTableLocation + "', '" + USERNAME + "', '"
	// + PASSWORD + "', '" + tableName + "');";
	// // tableName = REPLICAS;
	// // sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName +
	// "('org.h2.Driver', '" + systemTableLocation + "', '" + USERNAME + "', '"
	// + PASSWORD + "', '" + tableName + "');";
	//
	// //System.out.println("Linked table query: " + sql);
	//
	// return executeUpdate(sql);
	// }
	//
	// /**
	// * Contacts the System Table and accesses information on the set of
	// available remote tables.
	// * @param localMachineAddress The address of the local requesting machine
	// (so as to exclude local results)
	// * @param localMachinePort The port number of the local requesting machine
	// (so as to exclude local results)
	// * @param dbLocation The location of the database on the local machine.
	// * @return Result-set of all remote tables.
	// * @throws SQLException
	// */
	// public LocalResult getAllRemoteTables(String localMachineAddress, int
	// localMachinePort, String dbLocation) throws SQLException{
	// String sql =
	// "SELECT schemaname, tablename, db_location, connection_type, machine_name, connection_port "
	// +
	// "FROM " + REPLICAS + ", " + CONNECTIONS + ", " + TABLES +
	// " WHERE " + CONNECTIONS + ".connection_id = " + REPLICAS +
	// ".connection_id " +
	// "AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " +
	// "AND NOT (machine_name = '" + localMachineAddress +
	// "' AND connection_port = " + localMachinePort + " AND db_location = '" +
	// dbLocation + "');";
	//
	// return executeQuery(sql);
	// }

	/**
	 * Get the names of all the tables in a given schema.
	 * 
	 * @param schemaName
	 * @return
	 */
	public Set<String> getAllTablesInSchema(String schemaName) {
		String sql = "SELECT tablename FROM " + TABLES + " WHERE schemaname='"
				+ schemaName + "';";

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			Set<String> tableNames = new HashSet<String>();
			while (result.next()) {
				tableNames.add(result.currentRow()[0].getString());
			}

			return tableNames;

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the next available tableSet number. Not implemented in
	 * PersistentSystemTable.
	 * 
	 * @return
	 */
	public int getNewTableSetNumber() {
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.ISystemTable#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException {
		try {
			return isTableListed(ti);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.ISystemTable#lookup(java.lang.String)
	 */
	@Override
	public TableManagerWrapper lookup(TableInfo ti) throws RemoteException {
		//
		// /*
		// * Get the machine location of the table's Table Manager.
		// */
		// DatabaseURL dbURL = null;
		//
		// try {
		// dbURL = getTableManagerLocation(ti.getTableName(),
		// ti.getSchemaName());
		// } catch (SQLException e) {
		// e.printStackTrace();
		// return null;
		// }
		//
		// /*
		// * Use the chord interface to get a remote reference to this Table
		// Manager.
		// */
		//
		// IDatabaseRemote remoteInterface = db.getRemoteInterface();
		//
		// return remoteInterface.refindTableManagerReference(ti, dbURL);

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#buildSystemTableState(org.h2.h2o.manager
	 * .ISystemTable)
	 */
	@Override
	public void buildSystemTableState(ISystemTable otherSystemTable)
			throws RemoteException {
		/*
		 * Persist the state of the given System Table reference to disk.
		 */

		try {
			/*
			 * Obtain references to connected machines.
			 */
			Map<DatabaseURL, DatabaseInstanceWrapper> databasesInSystem = null;
			try {
				databasesInSystem = otherSystemTable.getConnectionInformation();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			for (Entry<DatabaseURL, DatabaseInstanceWrapper> databaseEntry : databasesInSystem
					.entrySet()) {
				try {
					addConnectionInformation(databaseEntry.getKey(),
							databaseEntry.getValue());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			/*
			 * Obtain references to Table Managers.
			 */

			Map<TableInfo, TableManagerWrapper> tableManagers = otherSystemTable
					.getTableManagers();

			for (Entry<TableInfo, TableManagerWrapper> dmEntry : tableManagers
					.entrySet()) {
				try {
					super.addTableInformation(dmEntry.getValue()
							.getTableManager().getDatabaseURL(),
							dmEntry.getKey(), false);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			/*
			 * Get table manager replica locations
			 */

			Map<TableInfo, Set<DatabaseURL>> replicaLocations = otherSystemTable
					.getReplicaLocations();
			Map<TableInfo, DatabaseURL> primaryLocations = otherSystemTable
					.getPrimaryLocations();

			for (Entry<TableInfo, Set<DatabaseURL>> databaseEntry : replicaLocations
					.entrySet()) {
				for (DatabaseURL tableInfo : databaseEntry.getValue()) {
					addTableManagerStateReplica(databaseEntry.getKey(),
							tableInfo,
							primaryLocations.get(databaseEntry.getKey()), false);
				}
			}

		} catch (MovedException e) {
			e.printStackTrace();
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation()
			throws RemoteException, SQLException {

		Map<DatabaseURL, DatabaseInstanceWrapper> databaseLocations = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();

		String sql = "SELECT * FROM " + CONNECTIONS + ";";

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while (result.next()) {
				/*
				 * sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" +
				 * "connection_id INT NOT NULL auto_increment," +
				 * "connection_type VARCHAR(5), " + "machine_name VARCHAR(255),"
				 * + "db_location VARCHAR(255)," +
				 * "connection_port INT NOT NULL, " + "rmi_port INT NOT NULL, "
				 * + "PRIMARY KEY (connection_id) );";
				 */
				Value[] row = result.currentRow();
				String connectionType = row[1].getString();
				String hostName = row[2].getString();
				String dbLocation = row[3].getString();
				int dbPort = row[4].getInt();
				int rmiPort = row[5].getInt();
				DatabaseURL dbURL = new DatabaseURL(connectionType, hostName,
						dbPort, dbLocation, false);
				dbURL.setRMIPort(rmiPort);
				databaseLocations.put(dbURL, null);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return databaseLocations;

		/*
		 * Parse connection information for every database. Does this include
		 * the RMI port?
		 */
	}

	private DatabaseURL getDatabaseURL(int replicaConnectionID) {
		DatabaseURL dbURL = null;

		String sql = "SELECT * FROM " + CONNECTIONS + " WHERE connection_id="
				+ replicaConnectionID + ";";

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while (result.next()) {
				/*
				 * sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" +
				 * "connection_id INT NOT NULL auto_increment," +
				 * "connection_type VARCHAR(5), " + "machine_name VARCHAR(255),"
				 * + "db_location VARCHAR(255)," +
				 * "connection_port INT NOT NULL, " + "rmi_port INT NOT NULL, "
				 * + "PRIMARY KEY (connection_id) );";
				 */
				Value[] row = result.currentRow();
				String connectionType = row[1].getString();
				String hostName = row[2].getString();
				String dbLocation = row[3].getString();
				int dbPort = row[4].getInt();
				int rmiPort = row[5].getInt();
				dbURL = new DatabaseURL(connectionType, hostName, dbPort,
						dbLocation, false);
				dbURL.setRMIPort(rmiPort);

			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return dbURL;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#getTableManagers()
	 */
	@Override
	public Map<TableInfo, TableManagerWrapper> getTableManagers()
			throws RemoteException {

		Map<TableInfo, TableManagerWrapper> tableManagers = new HashMap<TableInfo, TableManagerWrapper>();

		IDatabaseRemote remoteInterface = getDB().getRemoteInterface();

		/*
		 * Parse the query resultset to find the primary location of every
		 * table.
		 */
		String sql = "SELECT db_location, connection_type, machine_name, connection_port, tablename, schemaname, chord_port "
				+ "FROM "
				+ CONNECTIONS
				+ ", "
				+ TABLES
				+ " "
				+ "WHERE "
				+ CONNECTIONS
				+ ".connection_id = "
				+ TABLES
				+ ".manager_location;";

		// SELECT db_location, connection_type, machine_name, connection_port,
		// tablename, schemaname, chord_port FROM H2O.H2O_REPLICA,
		// H2O.H2O_CONNECTION, H2O.H2O_TABLE
		// WHERE H2O.H2O_TABLE.table_id=H2O.H2O_REPLICA.table_id AND
		// H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND
		// primary_copy = true;

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while (result != null && result.next()) {
				Value[] row = result.currentRow();

				String dbLocation = row[0].getString();
				String connectionType = row[1].getString();
				String machineName = row[2].getString();
				String connectionPort = row[3].getString();

				String tableName = row[4].getString();
				String schemaName = row[5].getString();
				int chord_port = row[6].getInt();

				DatabaseURL dbURL = new DatabaseURL(connectionType,
						machineName, Integer.parseInt(connectionPort),
						dbLocation, false, chord_port);
				TableInfo ti = new TableInfo(tableName, schemaName);

				/*
				 * Perform lookups to get remote references to every Table
				 * Manager.
				 */

				DatabaseInstanceRemote dir = null;
				try {
					dir = remoteInterface.getDatabaseInstanceAt(dbURL); // .findTableManagerReference(ti,
																		// dbURL);
				} catch (Exception e) {
					// Will happen if its no longer active.
				}

				if (dir != null) {
					TableManagerRemote dmReference = dir
							.findTableManagerReference(ti);

					TableManagerWrapper dmw = new TableManagerWrapper(ti,
							dmReference, dbURL);
					tableManagers.put(ti, dmw);
				} else {
					TableManagerWrapper dmw = new TableManagerWrapper(ti, null,
							dbURL);
					tableManagers.put(ti, dmw);
				}

			}

		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return tableManagers;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#getReplicaLocations()
	 */
	@Override
	public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations()
			throws RemoteException {
		/*
		 * Parse the schema tables to obtain the required amount of table
		 * information.
		 */

		String sql = "SELECT connection_id, tablename, schemaname    FROM "
				+ TABLEMANAGERSTATE + ", " + TABLES + " WHERE " + TABLES
				+ ".table_id" + "=" + TABLEMANAGERSTATE + ".table_id;";

		LocalResult result = null;

		Map<TableInfo, Set<DatabaseURL>> replicaLocations = new HashMap<TableInfo, Set<DatabaseURL>>();

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while (result != null && result.next()) {
				Value[] row = result.currentRow();

				int replicaConnectionID = row[0].getInt();

				String tableName = row[1].getString();
				String schemaName = row[2].getString();

				TableInfo ti = new TableInfo(tableName, schemaName);
				DatabaseURL replicaURL = getDatabaseURL(replicaConnectionID);
				// DatabaseURL primaryURL =
				// getDatabaseURL(primaryManagerConnectionID);

				/*
				 * Add this replica to the set of replica locations for this
				 * table.
				 */
				Set<DatabaseURL> specificTableReplicaLocations = replicaLocations
						.get(ti);
				if (specificTableReplicaLocations == null)
					specificTableReplicaLocations = new HashSet<DatabaseURL>();
				specificTableReplicaLocations.add(replicaURL);
				replicaLocations.put(ti, specificTableReplicaLocations);

			}

		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return replicaLocations;
	}

	@Override
	public Map<TableInfo, DatabaseURL> getPrimaryLocations()
			throws RemoteException, MovedException {
		/*
		 * Parse the schema tables to obtain the required amount of table
		 * information.
		 */

		String sql = "SELECT primary_location_connection_id, tablename, schemaname    FROM "
				+ TABLEMANAGERSTATE
				+ ", "
				+ TABLES
				+ " WHERE "
				+ TABLES
				+ ".table_id" + "=" + TABLEMANAGERSTATE + ".table_id;";

		LocalResult result = null;

		Map<TableInfo, DatabaseURL> primaryLocations = new HashMap<TableInfo, DatabaseURL>();

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while (result != null && result.next()) {
				Value[] row = result.currentRow();

				int primaryManagerConnectionID = row[0].getInt();
				String tableName = row[1].getString();
				String schemaName = row[2].getString();

				TableInfo ti = new TableInfo(tableName, schemaName);
				DatabaseURL primaryURL = getDatabaseURL(primaryManagerConnectionID);

				/*
				 * Add this replica to the set of replica locations for this
				 * table.
				 */

				primaryLocations.put(ti, primaryURL);
			}

		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return primaryLocations;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState()
	 */
	@Override
	public void buildSystemTableState() throws RemoteException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() throws RemoteException,
			MovedException {
		removeTableInformation(null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstance(org.h2.h2o.util.
	 * DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL)
			throws RemoteException, MovedException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceWrapper> getDatabaseInstances()
			throws RemoteException, MovedException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#changeTableManagerLocation(org.h2.h2o
	 * .comms.remote.TableManagerRemote)
	 */
	@Override
	public void changeTableManagerLocation(
			TableManagerRemote locationOfManager, TableInfo tableInfo) {
		super.changeTableManagerLocation(tableInfo);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#addTableInformation(org.h2.h2o.comms.
	 * remote.TableManagerRemote, org.h2.h2o.util.TableInfo)
	 */
	@Override
	public boolean addTableInformation(TableManagerRemote tableManager,
			TableInfo tableDetails,
			Set<DatabaseInstanceWrapper> replicaLocations)
			throws RemoteException, MovedException, SQLException {
		boolean added = super.addTableInformation(
				tableManager.getDatabaseURL(), tableDetails, false);

		if (added) {
			int connectionID = getConnectionID(tableDetails.getURL());
			addTableManagerReplicaInformationOnCreateTable(
					getTableID(tableDetails), connectionID, true,
					replicaLocations);
		}

		return added;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#addConnectionInformation(org.h2.h2o.util
	 * .DatabaseURL, org.h2.h2o.comms.remote.DatabaseInstanceWrapper)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL,
			DatabaseInstanceWrapper databaseInstanceWrapper)
			throws RemoteException, MovedException, SQLException {
		return super.addConnectionInformation(databaseURL,
				databaseInstanceWrapper.isActive());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#getLocalDatabaseInstances(org.h2.h2o.
	 * util.DatabaseURL)
	 */
	@Override
	public Set<TableManagerWrapper> getLocalDatabaseInstances(
			DatabaseURL localMachineLocation) throws RemoteException,
			MovedException {
		int connectionID = getConnectionID(localMachineLocation);

		assert connectionID != -1;

		String sql = "SELECT tablename, schemaname FROM " + TABLES
				+ "  WHERE manager_location= " + connectionID + ";";

		Set<TableManagerWrapper> localTables = new HashSet<TableManagerWrapper>();
		try {
			LocalResult rs = executeQuery(sql);

			while (rs.next()) {
				TableInfo tableInfo = new TableInfo(
						rs.currentRow()[0].getString(),
						rs.currentRow()[1].getString());
				TableManagerWrapper dmw = new TableManagerWrapper(tableInfo,
						null, null);
				localTables.add(dmw);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return localTables;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#addTableManagerStateReplica(org.h2.h2o
	 * .util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void addTableManagerStateReplica(TableInfo table,
			DatabaseURL replicaLocation, DatabaseURL primaryLocation,
			boolean active) throws RemoteException, MovedException {
		try {
			addTableManagerReplicaInformation(getTableID(table),
					getConnectionID(replicaLocation),
					getConnectionID(primaryLocation), active);
		} catch (SQLException e) {
			throw new RemoteException(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#removeTableManagerStateReplica(org.h2
	 * .h2o.util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void removeTableManagerStateReplica(TableInfo table,
			DatabaseURL replicaLocation) throws RemoteException, MovedException {
		try {
			removeTableManagerReplicaInformation(getTableID(table),
					getConnectionID(replicaLocation));
		} catch (SQLException e) {
			throw new RemoteException(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.PersistentManager#getLocation()
	 */
	@Override
	protected DatabaseURL getLocation() throws RemoteException {
		return this.getDB().getURL();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.h2o.manager.PersistentManager#getTableInfo()
	 */
	@Override
	protected TableInfo getTableInfo() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.h2.h2o.manager.ISystemTable#removeTableInformation(org.h2.h2o.util
	 * .TableInfo)
	 */
	@Override
	public boolean removeTableInformation(TableInfo ti) throws RemoteException,
			MovedException {
		return removeTableInformation(ti, false);
	}

	@Override
	public TableManagerRemote recreateTableManager(TableInfo table)
			throws RemoteException, MovedException {
		return null;
		// Done by in-memory system table.

	}

	@Override
	public boolean checkTableManagerAccessibility() {
		// Done by in-memory system table.
		return false;
	}

	@Override
	public Queue<DatabaseInstanceWrapper> getAvailableMachines(
			ActionRequest typeOfRequest) {
		// TODO Auto-generated method stub
		return null;
	}

}
