package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.remote.IDatabaseRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.value.Value;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PersistentSchemaManager extends PersistentManager implements ISchemaManager {

	/**
	 * Name of the schema used to store schema manager tables.
	 */
	private static final String SCHEMA = "H2O.";

	/**
	 * Name of tables' table in schema manager.
	 */
	private static final String TABLES = SCHEMA + "H2O_TABLE";

	/**
	 * Name of replicas' table in schema manager.
	 */
	private static final String REPLICAS = SCHEMA + "H2O_REPLICA";

	/**
	 * Name of connections' table in schema manager.
	 */
	private static final String CONNECTIONS = SCHEMA + "H2O_CONNECTION";

	/**
	 * The database username used to communicate with schema manager tables.
	 */
	public static final String USERNAME = "angus";

	/**
	 * The database password used to communicate with schema manager tables.
	 */
	public static final String PASSWORD = "";
	

	public PersistentSchemaManager(Database db, boolean createTables) throws Exception{
		super (db, createTables, TABLES, REPLICAS, CONNECTIONS);
	}

	public DatabaseURL getDataManagerLocation(String tableName, String schemaName) throws SQLException{
		String sql = "SELECT db_location, connection_type, machine_name, connection_port " +
		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
		"WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";

		LocalResult result = null;

		sqlQuery = getParser().prepareCommand(sql);

		try {

			result = sqlQuery.executeQueryLocal(1);

		} catch (Exception e){
			e.printStackTrace();
		}

		if (result != null && result.next()){
			Value[] row = result.currentRow();

			String dbLocation = row[0].getString();
			String connectionType = row[1].getString();
			String machineName = row[2].getString();
			String connectionPort = row[3].getString();

			DatabaseURL dbURL = new DatabaseURL(connectionType, machineName, Integer.parseInt(connectionPort), dbLocation, false);

			return dbURL;
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Looking for table: " + schemaName + "." + tableName + " (but it wasn't found).");
		throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);


	}



	
	/**
	 * Creates linked tables for a remote schema manager, location specified by the paramter.
	 * @param schemaManagerLocation Location of the remote schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public int createLinkedTablesForSchemaManager(String schemaManagerLocation) throws SQLException{
		String sql = "CREATE SCHEMA IF NOT EXISTS H2O;";
		String tableName = TABLES;
		sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', '" + USERNAME + "', '" + PASSWORD + "', '" + tableName + "');";
		tableName = CONNECTIONS;
		sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', '" + USERNAME + "', '" + PASSWORD + "', '" + tableName + "');";
		tableName = REPLICAS;
		sql += "\nCREATE LINKED TABLE IF NOT EXISTS " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', '" + USERNAME + "', '" + PASSWORD + "', '" + tableName + "');";

		//System.out.println("Linked table query: " + sql);

		return executeUpdate(sql);
	}

	/**
	 * Contacts the schema manager and accesses information on the set of available remote tables.
	 * @param localMachineAddress	The address of the local requesting machine (so as to exclude local results)
	 * @param localMachinePort	The port number of the local requesting machine (so as to exclude local results)
	 * @param dbLocation The location of the database on the local machine.
	 * @return	Result-set of all remote tables.
	 * @throws SQLException
	 */
	public LocalResult getAllRemoteTables(String localMachineAddress, int localMachinePort, String dbLocation) throws SQLException{
		String sql = "SELECT schemaname, tablename, db_location, connection_type, machine_name, connection_port " +
		"FROM " + REPLICAS + ", " + CONNECTIONS + ", " + TABLES +
		" WHERE " + CONNECTIONS + ".connection_id = " + REPLICAS + ".connection_id " +
		"AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " +
		"AND NOT (machine_name = '" + localMachineAddress + "' AND connection_port = " + localMachinePort + " AND db_location = '" + dbLocation + "');";

		return executeQuery(sql);
	}




	
	/**
	 * Get the names of all the tables in a given schema.
	 * @param schemaName
	 * @return
	 */
	public Set<String> getAllTablesInSchema(String schemaName) {
		String sql = "SELECT tablename FROM " + TABLES + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);


			result = sqlQuery.executeQueryLocal(0);

			Set<String> tableNames = new HashSet<String>();
			while (result.next()){
				tableNames.add(result.currentRow()[0].getString());
			}

			return tableNames;

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Returns the next available tableSet number.
	 * @return
	 */
	public int getNewTableSetNumber() {
		//		Random rnd = new Random();
		//		int val = rnd.nextInt(100);
		String sql = "SELECT (max(table_set)+1) FROM " + REPLICAS + ";";

		LocalResult result = null;
		try {
			sqlQuery = getParser().prepareCommand(sql);


			result = sqlQuery.executeQueryLocal(1);


			if (result.next()){
				return result.currentRow()[0].getInt();
			}

			return -1;

		} catch (SQLException e) {
			e.printStackTrace();

			return -1;
		}

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#exists(java.lang.String)
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

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#lookup(java.lang.String)
	 */
	@Override
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException {
		//
		//		/*
		//		 * Get the machine location of the table's data manager.
		//		 */
		//		DatabaseURL dbURL = null;
		//
		//		try {
		//			dbURL = getDataManagerLocation(ti.getTableName(), ti.getSchemaName());
		//		} catch (SQLException e) {
		//			e.printStackTrace();
		//			return null;
		//		}
		//
		//		/*
		//		 * Use the chord interface to get a remote reference to this data manager.
		//		 */
		//
		//		IDatabaseRemote remoteInterface = db.getRemoteInterface();
		//
		//		return remoteInterface.refindDataManagerReference(ti, dbURL);

		return null;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager)
	throws RemoteException {
		/*
		 * Persist the state of the given schema manager reference to disk.
		 */

		try {		
			/*
			 * Obtain references to connected machines.
			 */
			Map<DatabaseURL, DatabaseInstanceWrapper> databasesInSystem = null;
			try {
				databasesInSystem = otherSchemaManager.getConnectionInformation();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			for (Entry<DatabaseURL, DatabaseInstanceWrapper> databaseEntry: databasesInSystem.entrySet()){
				try {
					addConnectionInformation(databaseEntry.getKey(), databaseEntry.getValue());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			/*
			 * Obtain references to data managers.
			 */

			Map<TableInfo, DataManagerRemote> dataManagers = otherSchemaManager.getDataManagers();

			for (Entry<TableInfo, DataManagerRemote> dmEntry: dataManagers.entrySet()){
				try {
					addTableInformation(dmEntry.getValue(), dmEntry.getKey());
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			/*
			 * Obtain references to replicas.
			 */

			Map<String, Set<TableInfo>> replicaLocations = otherSchemaManager.getReplicaLocations();

			for (Entry<String, Set<TableInfo>> databaseEntry: replicaLocations.entrySet()){
				for (TableInfo tableInfo: databaseEntry.getValue()){
					try {
						addReplicaInformation(tableInfo);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (MovedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, SQLException {

		Map<DatabaseURL, DatabaseInstanceWrapper> databaseLocations = new HashMap<DatabaseURL, DatabaseInstanceWrapper>();

		String sql = "SELECT * FROM " + CONNECTIONS + ";";

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);


			result = sqlQuery.executeQueryLocal(0);

			while(result.next()){
				/*
				 * 		sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + 
						"connection_id INT NOT NULL auto_increment," + 
						"connection_type VARCHAR(5), " + 
						"machine_name VARCHAR(255)," + 
						"db_location VARCHAR(255)," +
						"connection_port INT NOT NULL, " + 
						"rmi_port INT NOT NULL, " + 
						"PRIMARY KEY (connection_id) );";
				 */
				Value[] row = result.currentRow();
				String connectionType = row[1].getString();
				String hostName = row[2].getString();
				String dbLocation = row[3].getString();
				int dbPort = row[4].getInt();
				int rmiPort = row[5].getInt();
				DatabaseURL dbURL = new DatabaseURL(connectionType, hostName, dbPort, dbLocation, false);
				dbURL.setRMIPort(rmiPort);
				databaseLocations.put(dbURL, null);
			}


		} catch (SQLException e) {
			e.printStackTrace();
		}

		return databaseLocations;

		/*
		 * Parse connection information for every database. Does this include the RMI port?
		 */
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDataManagers()
	 */
	@Override
	public Map<TableInfo, DataManagerRemote> getDataManagers()  throws RemoteException{

		Map<TableInfo, DataManagerRemote> dataManagers = new HashMap<TableInfo, DataManagerRemote>();

		IDatabaseRemote remoteInterface = getDB().getRemoteInterface();

		/*
		 * Parse the query resultset to find the primary location of every table.
		 */
		String sql = "SELECT db_location, connection_type, machine_name, connection_port, tablename, schemaname, chord_port " +
		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
		"WHERE " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";


		//		SELECT db_location, connection_type, machine_name, connection_port, tablename, schemaname, chord_port FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE 
		//		WHERE H2O.H2O_TABLE.table_id=H2O.H2O_REPLICA.table_id AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;

		LocalResult result = null;

		try {
			sqlQuery = getParser().prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(0);

			while(result != null && result.next()){
				Value[] row = result.currentRow();

				String dbLocation = row[0].getString();
				String connectionType = row[1].getString();
				String machineName = row[2].getString();
				String connectionPort = row[3].getString();

				String tableName = row[4].getString();
				String schemaName = row[5].getString();
				int chord_port = row[6].getInt();

				DatabaseURL dbURL = new DatabaseURL(connectionType, machineName, Integer.parseInt(connectionPort), dbLocation, false);
				dbURL.setRMIPort(chord_port);
				TableInfo ti = new TableInfo (tableName, schemaName);

				/*
				 * Perform lookups to get remote references to every data manager.
				 */
				DatabaseInstanceRemote dir = remoteInterface.getDatabaseInstanceAt(dbURL);   //.findDataManagerReference(ti, dbURL);

				if (dir != null){
					DataManagerRemote dmReference = dir.findDataManagerReference(ti);
					dataManagers.put(ti, dmReference);
				} else {
					dataManagers.put(ti, null);
				}

			}

		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return dataManagers;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations()  throws RemoteException{
		// TODO Auto-generated method stub

		/*
		 * Parse the schema tables to obtain the required amount of table information.
		 */
		return null;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState()
	 */
	@Override
	public void buildSchemaManagerState() throws RemoteException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() {
		// TODO Auto-generated method stub

	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL)
	throws RemoteException, MovedException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceWrapper> getDatabaseInstances()
	throws RemoteException, MovedException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#changeDataManagerLocation(org.h2.h2o.comms.remote.DataManagerRemote)
	 */
	@Override
	public void changeDataManagerLocation(DataManagerRemote stub, TableInfo tableInfo) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#addTableInformation(org.h2.h2o.comms.remote.DataManagerRemote, org.h2.h2o.util.TableInfo)
	 */
	@Override
	public boolean addTableInformation(DataManagerRemote dataManager,
			TableInfo tableDetails) throws RemoteException, MovedException, SQLException {
		return super.addTableInformation(dataManager.getDatabaseURL(), tableDetails);
		}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#addConnectionInformation(org.h2.h2o.util.DatabaseURL, org.h2.h2o.comms.remote.DatabaseInstanceWrapper)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL,
			DatabaseInstanceWrapper databaseInstanceWrapper)
			throws RemoteException, MovedException, SQLException {
		return super.addConnectionInformation(databaseURL, databaseInstanceWrapper.isActive());
	}
}
