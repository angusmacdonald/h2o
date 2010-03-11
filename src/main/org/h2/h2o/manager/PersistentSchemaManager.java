package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.remote.IDatabaseRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.value.Value;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PersistentSchemaManager implements ISchemaManager{

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


	/**
	 * Query parser instance to be used for all queries to the schema manager.
	 */
	private Parser queryParser;
	private Command sqlQuery;

	private Database db;

	private int cacheConnectionID;

	private ReplicaManager replicaManager;

	public PersistentSchemaManager(Database db, boolean schemaTablesExist) throws Exception{

		this.db = db;

		queryParser = new Parser(db.getSystemSession(), true);

		this.replicaManager = new ReplicaManager();

		replicaManager.add(db.getLocalDatabaseInstance());

		if (!schemaTablesExist){
			/*
			 * Create a new set of schema tables locally.
			 */
			try {
				setupSchemaManagerStateTables();
			} catch (SQLException e) {
				throw new Exception("Couldn't create schema manager state tables.");
			}

		} else {
			/*
			 * A local copy of the schema already exists locally. There may be other remote copies,
			 * but they are not known, and consequently not active.
			 */

			//This currently does nothing, but could in future look for remote copies, or create remote replicas.
		}

		//replicaManager.add(db.getLocalDatabaseInstance());

	}

	/**
	 * Creates the set of tables used by the schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	private int setupSchemaManagerStateTables() throws SQLException{

		String sql = "CREATE SCHEMA IF NOT EXISTS H2O; " +
		"\n\nCREATE TABLE IF NOT EXISTS " + TABLES + "( table_id INT NOT NULL auto_increment, " +
		"schemaname VARCHAR(255), tablename VARCHAR(255), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (table_id) );";

		sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + 
		"connection_id INT NOT NULL auto_increment," + 
		"connection_type VARCHAR(5), " + 
		"machine_name VARCHAR(255)," + 
		"db_location VARCHAR(255)," +
		"connection_port INT NOT NULL, " + 
		"chord_port INT NOT NULL, " + 
		"PRIMARY KEY (connection_id) );";

		sql += "\n\nCREATE TABLE IF NOT EXISTS " + REPLICAS + "(" +
		"replica_id INT NOT NULL auto_increment, " +
		"table_id INT NOT NULL, " +
		"connection_id INT NOT NULL, " + 
		"storage_type VARCHAR(255), " + 
		"last_modification INT NOT NULL, " +
		"table_set INT NOT NULL, " +
		"primary_copy BOOLEAN, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (table_id) REFERENCES " + TABLES + " (table_id) ON DELETE CASCADE , " +
		" FOREIGN KEY (connection_id) REFERENCES " + CONNECTIONS + " (connection_id));";


		sqlQuery = queryParser.prepareCommand(sql);

		int result = -1;
		try {
			result = sqlQuery.update();

			sqlQuery.close();
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return result;
	}

	public DatabaseURL getDataManagerLocation(String tableName, String schemaName) throws SQLException{
		String sql = "SELECT db_location, connection_type, machine_name, connection_port " +
		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
		"WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

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
	 * Add a new table to the schema manager. Called at the end of a CreateTable update. 
	 * @param tableName				Name of the table being added.
	 * @param modificationId		Modification ID of the table.
	 * @param databaseLocation		Location of the table (the database it is stored in)
	 * @param tableType				Type of the table (e.g. Linked, View, Table).
	 * @param localMachineAddress	Address through which the DB is contactable.
	 * @param localMachinePort		Port the server is running on.
	 * @param connection_type		The type of connection (e.g. TCP, FTP).
	 * @throws SQLException 
	 */
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails){

		getNewQueryParser();

		try {
			String tableName = tableDetails.getTableName();
			String schemaName = tableDetails.getSchemaName();


			if (!isTableListed(tableDetails)){ // the table doesn't already exist in the schema manager.
				addTableInformation(tableName,  tableDetails.getModificationID(), schemaName);
			}


			DatabaseURL dbURL = tableDetails.getDbLocation();

			int connectionID = getConnectionID(dbURL);

			assert connectionID != -1;

			int tableID = getTableID(tableDetails);
			if (!isReplicaListed(tableName, connectionID, dbURL.getDbLocation(), schemaName)){ // the table doesn't already exist in the schema manager.
				addReplicaInformation(tableDetails, tableID, connectionID, true);				
			}
			return true;
		} catch (SQLException e) {

			e.printStackTrace();
			return false;
		}
	}


	/**
	 * Add a new replica to the schema manager. The table already exists, so it is assumed there is an entry for that table
	 * in the schema manager. This method only updates the replica table in the schema manager.
	 * @param tableName				Name of the table being added.
	 * @param modificationId		Modification ID of the table.
	 * @param databaseLocation		Location of the table (the database it is stored in)
	 * @param tableType				Type of the table (e.g. Linked, View, Table).
	 * @param localMachineAddress	Address through which the DB is contactable.
	 * @param localMachinePort		Port the server is running on.
	 * @param connection_type		The type of connection (e.g. TCP, FTP).
	 */
	public void addReplicaInformation(TableInfo tableDetails) {
		//getNewQueryParser();

		//queryParser = new Parser(db.getExclusiveSession(), true);

		try {
			int connectionID = getConnectionID(tableDetails.getDbLocation());
			int tableID = getTableID(tableDetails);

			if (!isReplicaListed(tableDetails.getTableName(), connectionID, tableDetails.getDbLocation().getDbLocation(), tableDetails.getSchemaName())){ // the table doesn't already exist in the schema manager.
				addReplicaInformation(tableDetails, tableID, connectionID, false);				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	private void getNewQueryParser() {
		Session s = null;

		if (db.getSessions(false).length > 0){
			s = db.getSessions(false)[0];
		} else {
			s = db.getSystemSession();
		}

		queryParser = new Parser(s, true);

	}

	/**
	 * Check if the schema manager contains connection information for this database.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @return						True, if the connection information already exists.
	 * @throws SQLException
	 */
	public boolean connectionInformationExists(String localMachineAddress, int localMachinePort, String connectionType, String databaseLocation) throws SQLException{
		String sql = "SELECT count(connection_id) FROM " + CONNECTIONS + " WHERE machine_name='" + localMachineAddress + "' AND db_location='" + databaseLocation +
		"' AND connection_port=" + localMachinePort + " AND connection_type='" + connectionType +"';";

		return countCheck(sql);
	}

	/**
	 * Update the schema manager with new connection information.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @param databaseLocation 		The location of the local database. Used to determine whether a database in running in embedded mode.
	 * @return						Result of the update.
	 */
	public int addConnectionInformation(DatabaseURL dburl){
		String connection_type = dburl.getConnectionType();

		String sql = null;
		try {
			if (connectionInformationExists(dburl.getHostname(), dburl.getPort(), dburl.getConnectionType(), dburl.getDbLocation())){
				//Update existing information - the chord port may have changed.

				sql = "\nUPDATE " + CONNECTIONS + " SET chord_port = " + dburl.getRMIPort() + 
				" WHERE machine_name='" + dburl.getHostname() + "' AND connection_port=" + dburl.getPort() +
				" AND connection_type='" + dburl.getConnectionType() +"';";

			} else { 

				sql = "\nINSERT INTO " + CONNECTIONS + " VALUES (null, '" + connection_type + "', '" + dburl.getHostname() + 
				"', '" + dburl.getDbLocation() + "', "  + dburl.getPort() + ", " +
				dburl.getRMIPort() + ");\n";

			}
		} catch (SQLException e1) {
			e1.printStackTrace();
			return -1;
		}

		try {
			return executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
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
	 * Gets the number of replicas that exist for a given table.
	 */
	public int getNumberofReplicas(String tableName, String schemaName){
		String sql = "SELECT count(*) FROM " + REPLICAS + ", " + TABLES + " WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND" +
		" " + TABLES + ".table_id=" + REPLICAS + ".table_id;";

		try {
			return getCount(sql);
		} catch (SQLException e) {
			e.printStackTrace();

			return -1;
		}
	}

	/**
	 * Gets the connection ID for a given database connection if it is present in the database. If not, -1 is returned.
	 * 
	 * Note: Currently, it is assumed this is only called by the local machine (only current use), so the connection_id is cached
	 * and returned if it is already known.
	 * 
	 * @param machine_name
	 * @param connection_port
	 * @param connection_type
	 * @return
	 */
	public int getConnectionID(DatabaseURL dbURL){
		String machine_name = dbURL.getHostname();
		int connection_port = dbURL.getPort();
		String connection_type = dbURL.getConnectionType();

		//		if (cacheConnectionID != -1)
		//			return cacheConnectionID;

		String sql = "SELECT connection_id FROM " + CONNECTIONS + " WHERE machine_name='" + machine_name
		+ "' AND connection_port=" + connection_port + " AND connection_type='" + connection_type + "' AND db_location = '" + dbURL.getDbLocation() + "';";


		
		LocalResult result = null;
		try {
			sqlQuery = queryParser.prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(1);

			if (result.next()){
				cacheConnectionID = result.currentRow()[0].getInt();
				return cacheConnectionID;
			} else {
				ErrorHandling.errorNoEvent("No connection ID was found - this shouldn't happen if the system has started correctly.");
				return -1;
			}


		} catch (SQLException e) {
			return -1;
		}
	}

	/**
	 *  Gets the table ID for a given database table if it is present in the database. If not, an exception is thrown.
	 * @param tableName		Name of the table
	 * @param schemaName	Schema the table is in.
	 * @return
	 */
	public int getTableID(TableInfo ti) throws SQLException{

		String sql = "SELECT table_id FROM " + TABLES + " WHERE tablename='" + ti.getTableName()
		+ "' AND schemaname='" + ti.getSchemaName() + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()){
			return result.currentRow()[0].getInt();
		} else {
			throw new SQLException("Internal problem: tableID not found in schema manager.");
		}


	}


	/**
	 * A check whether a table is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param schemaName 
	 * @return	true, if the table's information is already in the schema manager.
	 * @throws SQLException 
	 */
	public boolean isTableListed(TableInfo ti) throws SQLException{
		String sql =  "SELECT count(*) FROM " + TABLES + " WHERE tablename='" + ti.getTableName() + "' AND schemaname='" + ti.getSchemaName() +"';";

		return countCheck(sql);
	}

	/**
	 * A check whether a replica is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param connectionID		Connection ID of the machine holding this replica.
	 * @return	true, if the replica's information is already in the schema manager.
	 * @throws SQLException 
	 */
	public boolean isReplicaListed(String tableName, int connectionID, String dbLocation, String schemaName) throws SQLException{
		String sql = "SELECT count(*) FROM " + REPLICAS + ", " + TABLES + ", " + CONNECTIONS + " WHERE tablename='" + tableName + "' AND schemaname='" + 
		schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id AND " + REPLICAS + ".connection_id = " + CONNECTIONS + ".connection_id AND db_location='"
		+ dbLocation + "';";

		return countCheck(sql);
	}

	/**
	 * Takes in an SQL count(*) query, which should return a single result, which is a single integer, indicating
	 * the number of occurences of a given entry. If the number of entries is greater than zero true is returned; otherwise false.
	 * @param query	SQL count query.
	 * @return
	 * @throws SQLException
	 */
	private boolean countCheck(String query) throws SQLException{
		LocalResult result = executeQuery(query);
		if (result.next()){
			int count = result.currentRow()[0].getInt();

			return (count>0);
		}
		return false;
	}

	/**
	 * Takes in an SQL count(*) query, which should return a single result, which is a single integer, indicating
	 * the number of occurences of a given entry. If no count is returned from the ResultSet, -1 is returned from this method.
	 * @param query	SQL count query.
	 * @return
	 * @throws SQLException
	 */
	private int getCount(String query) throws SQLException{
		LocalResult result = executeQuery(query);
		if (result.next()){
			int count = result.currentRow()[0].getInt();

			return count;
		}
		return -1;
	}


	/**
	 * Update the schema manager with new table information
	 * @param tableName			Name of the table being added.
	 * @param modificationID	Mofification ID of the table.
	 * @param session 
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addTableInformation(String tableName, long modificationID, String schemaName) throws SQLException{
		String sql = "INSERT INTO " + TABLES + " VALUES (null, '" + schemaName + "', '" + tableName + "', " + modificationID +");";
		return executeUpdate(sql);
	}

	/**
	 * Update the schema manager with new replica information
	 * @param tableID			Name of the replica being added.
	 * @param modificationID	Mofification ID of the table.
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addReplicaInformation(TableInfo ti, int tableID, int connectionID, boolean primaryCopy) throws SQLException{
		String sql = "INSERT INTO " + REPLICAS + " VALUES (null, " + tableID + ", " + connectionID + ", '" + 
		ti.getTableType() + "', " + ti.getModificationID() +", " + ti.getTableSet() + ", " + primaryCopy + ");\n";
		return executeUpdate(sql);
	}

	/**
	 * Removes a table completely from the schema manager. Information is removed for the table itself and for all replicas.
	 * @param tableName		Leave null if you want to drop the entire schema.
	 * @param schemaName 
	 * @throws SQLException 
	 */
	public boolean removeTableInformation(TableInfo ti) {
		try {

			String sql = "";
			if (ti.getTableName() == null){
				/*
				 * Deleting the entire schema.
				 */
				Integer[] tableIDs;

				tableIDs = getTableIDs(ti.getSchemaName());


				if (tableIDs.length > 0){
					sql = "DELETE FROM " + REPLICAS;
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

				sql += "\nDELETE FROM " + TABLES + " WHERE schemaname='" + ti.getSchemaName() + "'; ";
			} else {

				int tableID = getTableID(ti);

				sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + ";";

				sql += "\nDELETE FROM " + TABLES + " WHERE table_id=" + tableID + "; ";

			}
			executeUpdate(sql);


			//int id = getTableID(new TableInfo("TEST", "PUBLIC"));

			return true;
		} catch (SQLException e) {
			e.printStackTrace();

			return false;
		}
	}


	/**
	 * Gets all the table IDs for tables in a schema.
	 * @param schemaName
	 * @return
	 * @throws SQLException 
	 */
	private Integer[] getTableIDs(String schemaName) throws SQLException {
		String sql = "SELECT table_id FROM " + TABLES + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(0);

		Set<Integer> ids = new HashSet<Integer>();
		while (result.next()){
			ids.add(result.currentRow()[0].getInt());
		}

		return ids.toArray(new Integer[0]);
	}

	private LocalResult executeQuery(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);

		return sqlQuery.executeQueryLocal(0);
	}

	private int executeUpdate(String query) throws SQLException{
		//getNewQueryParser();


		Set<DatabaseInstanceRemote> replicas = replicaManager.getActiveReplicas();

		int result = -1;

		for (DatabaseInstanceRemote replica: replicas){

			if (replica.equals(db.getLocalDatabaseInstance())){
						
						sqlQuery = queryParser.prepareCommand(query);
				
						try {
							result = sqlQuery.update();
				
							sqlQuery.close();
						} catch (RemoteException e) {
							e.printStackTrace();
						}
			} else {
				try {
					result = replica.executeUpdate(query);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}
		//		
		//		sqlQuery = queryParser.prepareCommand(query);
		//
		//		int result = -1;
		//		try {
		//			result = sqlQuery.update();
		//
		//			sqlQuery.close();
		//		} catch (RemoteException e) {
		//			e.printStackTrace();
		//			return -1;
		//		}

		return result;
	}

	/**
	 * Removes a particular replica from the schema manager. 
	 * @param tableName
	 * @param dbLocation 
	 * @param machineName 
	 * @param connectionPort 
	 * @param connectionType 
	 * @param schemaName 
	 * @throws SQLException 
	 */
	public void removeReplicaInformation(TableInfo ti) {

		try {

			int connectionID = getConnectionID(ti.getDbLocation());
			int tableID;

			tableID = getTableID(ti);

			String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + " AND connection_id=" + connectionID  + "; ";

			executeUpdate(sql);

		} catch (SQLException e) {
			e.printStackTrace();
		}
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
			sqlQuery = queryParser.prepareCommand(sql);


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
			sqlQuery = queryParser.prepareCommand(sql);


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

		/*
		 * Get the machine location of the table's data manager.
		 */
		DatabaseURL dbURL = null;

		try {
			dbURL = getDataManagerLocation(ti.getTableName(), ti.getSchemaName());
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		/*
		 * Use the chord interface to get a remote reference to this data manager.
		 */

		IDatabaseRemote remoteInterface = db.getRemoteInterface();

		return remoteInterface.refindDataManagerReference(ti, dbURL);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager)
	throws RemoteException {
		// TODO Auto-generated method stub

		/*
		 * Persist the state of the given schema manager reference to disk.
		 */
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getConnectionInformation()
	 */
	@Override
	public Set<DatabaseURL> getConnectionInformation() throws RemoteException {

		Set<DatabaseURL> databaseLocations = new HashSet<DatabaseURL>();

		String sql = "SELECT * FROM " + CONNECTIONS + ";";

		LocalResult result = null;

		try {
			sqlQuery = queryParser.prepareCommand(sql);


			result = sqlQuery.executeQueryLocal(0);

			while(result.next()){
				/*
				 * 		sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + 
						"connection_id INT NOT NULL auto_increment," + 
						"connection_type VARCHAR(5), " + 
						"machine_name VARCHAR(255)," + 
						"connection_port INT NOT NULL, " + 
						"rmi_port INT NOT NULL, " + 
						"PRIMARY KEY (connection_id) );";
				 */
				Value[] row = result.currentRow();
				String connectionType = row[1].getString();
				String hostName = row[2].getString();
				int dbPort = row[3].getInt();
				int rmiPort = row[4].getInt();
				DatabaseURL dbURL = new DatabaseURL(connectionType, hostName, dbPort, null, false);
				dbURL.setRMIPort(rmiPort);
				databaseLocations.add(dbURL);
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

		IDatabaseRemote remoteInterface = db.getRemoteInterface();

		/*
		 * Parse the query resultset to find the primary location of every table.
		 */
		String sql = "SELECT db_location, connection_type, machine_name, connection_port, tablename, schemaname, chord_port " +
		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
		"WHERE " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";

		LocalResult result = null;

		try {
			sqlQuery = queryParser.prepareCommand(sql);

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
				DataManagerRemote dmReference = remoteInterface.refindDataManagerReference(ti, dbURL);

				dataManagers.put(ti, dmReference);



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
	 * @see org.h2.h2o.manager.ISchemaManager#addSchemaManagerDataLocation(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void addSchemaManagerDataLocation(
			DatabaseInstanceRemote databaseReference) throws RemoteException {
		replicaManager.add(databaseReference);
		
		//TODO now replica state here.
		databaseReference.executeUpdate("CREATE REPLICA " + TABLES + ", " + REPLICAS + ", " + CONNECTIONS + " FROM '" + db.getDatabaseURL().getOriginalURL() + "';");
	}


}
