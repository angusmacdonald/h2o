package org.h2.engine;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.value.Value;

/**
 * Contains various utility methods which the system can use to access and modify H2O's schema manager. SQL for the tables is as follows:<code>
		CREATE SCHEMA IF NOT EXISTS H2O;<br/>

		CREATE TABLE IF NOT EXISTS H2O.H2O_TABLE(
		    table_id INT NOT NULL auto_increment,
		    schemaname VARCHAR(255),
		    tablename VARCHAR(255), 
			last_modification INT NOT NULL,
			PRIMARY KEY (table_id)
		);<br/>

		CREATE TABLE IF NOT EXISTS H2O.H2O_REPLICA(
		    replica_id INT NOT NULL auto_increment,
			table_id INT NOT NULL, 
			connection_id INT NOT NULL,
			db_location VARCHAR(255),
			storage_type VARCHAR(50),
			last_modification INT NOT NULL,
			table_set INT NOT NULL,
			PRIMARY KEY (replica_id),
			FOREIGN KEY (table_id) REFERENCES H2O.H2O_TABLE (table_id),
			FOREIGN KEY (connection_id) REFERENCES H2O.H2O_CONNECTION (connection_id)
		);<br/>

		CREATE TABLE IF NOT EXISTS H2O.H2O_CONNECTION(
		    connection_id INT NOT NULL auto_increment,
			machine_name VARCHAR(255), 
			connection_type VARCHAR(5), 
			connection_port INT NOT NULL, 
			PRIMARY KEY (connection_id),
		); <br/></code>

 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManager {

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
	public static String USERNAME = "angus";

	/**
	 * The database password used to communicate with schema manager tables.
	 */
	public static String PASSWORD = "supersecret";


	/**
	 * Query parser instance to be used for all queries to the schema manager.
	 */
	private Parser queryParser;
	private Command sqlQuery;

	/**
	 * A cache of the local connection_id, to be used when adding new tables on this machine.
	 */
	private int cacheConnectionID;

	private static SchemaManager singleton = null;

	public static SchemaManager getInstance(){
		if (singleton == null){
			try {
				throw new Exception("Unexpected code path.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return singleton;
	}

	/**
	 * Returns the singleton instance of SchemaManager. If the instance has not yet
	 * been created it will be, using the Session instance provided.
	 * @return
	 */
	public static SchemaManager getInstance(Session session){
		if (singleton == null){
			singleton = new SchemaManager(session);
		} else {
			singleton.queryParser = new Parser(session);
		}

		return singleton;
	}

	private SchemaManager(Session session) {
		queryParser = new Parser(session);

		cacheConnectionID = -1;
	}

	/**
	 * Creates the set of tables used by the schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public int createSchemaManagerTables() throws SQLException{

		String sql = "CREATE SCHEMA IF NOT EXISTS H2O; " +
		"\n\nCREATE TABLE IF NOT EXISTS " + TABLES + "( table_id INT NOT NULL auto_increment, " +
		"schemaname VARCHAR(255), tablename VARCHAR(255), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (table_id) );";

		sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + 
		"connection_id INT NOT NULL auto_increment," + 
		"connection_type VARCHAR(5), " + 
		"machine_name VARCHAR(255)," + 
		"connection_port INT NOT NULL, " + 
		"PRIMARY KEY (connection_id) );";

		sql += "\n\nCREATE TABLE IF NOT EXISTS " + REPLICAS + "(replica_id INT NOT NULL auto_increment, " +
		"table_id INT NOT NULL, " +
		"connection_id INT NOT NULL, " + 
		"db_location VARCHAR(255)," +
		"storage_type VARCHAR(50), " +
		"last_modification INT NOT NULL, " +
		"table_set INT NOT NULL, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (table_id) REFERENCES " + TABLES + " (table_id) ON DELETE CASCADE , " +
		" FOREIGN KEY (connection_id) REFERENCES " + CONNECTIONS + " (connection_id));\n";

		return executeUpdate(sql);
	}

	public String getPrimaryReplicaLocation(String tableName, String schemaName) throws SQLException{
		String sql = "SELECT db_location, connection_type, machine_name, connection_port " +
		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
		"WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id;";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		try {

			result = sqlQuery.executeQueryLocal(1);

		} catch (Exception e){
			e.printStackTrace();
		}

		if (result != null && result.next()){ //XXX This just takes the first replica, assuming there are more than one.
			Value[] row = result.currentRow();

			String dbLocation = row[0].getString();
			String connectionType = row[1].getString();
			String machineName = row[2].getString();
			String connectionPort = row[3].getString();

			String dbname = null;
			if (connectionType.equals("tcp")){
				dbname = "jdbc:h2:" + connectionType + "://" + machineName + ":" + connectionPort + "/" + dbLocation;
			} else if (connectionType.equals("mem")){
				dbname = "jdbc:h2:" + dbLocation;
			} else {
				Message.throwInternalError("This connection type isn't supported yet. Get on that!");
			}

			return dbname;
		}

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
	public void addTableInformation(String tableName, long modificationID,
			String databaseLocation, String tableType,
			String localMachineAddress, int localMachinePort, String connection_type, String schemaName, int tableSet) throws SQLException {

		if (!isTableListed(tableName, schemaName)){ // the table doesn't already exist in the schema manager.
			addTableInformation(tableName,  modificationID, schemaName);
		}

		int connectionID = getConnectionID(localMachineAddress, localMachinePort, connection_type);
		int tableID = getTableID(tableName, schemaName);
		if (!isReplicaListed(tableName, connectionID, databaseLocation, schemaName)){ // the table doesn't already exist in the schema manager.
			addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, tableSet);				
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
	 * @throws SQLException 
	 */
	public void addReplicaInformation(String tableName, long modificationID,
			String databaseLocation, String tableType,
			String localMachineAddress, int localMachinePort, String connection_type, String schemaName, int replicaSet) throws SQLException {

		int connectionID = getConnectionID(localMachineAddress, localMachinePort, connection_type);
		int tableID = getTableID(tableName, schemaName);

		if (!isReplicaListed(tableName, connectionID, databaseLocation, schemaName)){ // the table doesn't already exist in the schema manager.
			addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, replicaSet);				
		}
	}

	/**
	 * Check if the schema manager contains connection information for this database.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @return						True, if the connection information already exists.
	 * @throws SQLException
	 */
	public boolean connectionInformationExists(String localMachineAddress, int localMachinePort) throws SQLException{
		String sql = "SELECT count(connection_id) FROM " + CONNECTIONS + " WHERE machine_name='" + localMachineAddress + "' AND connection_port=" + localMachinePort +
		" AND connection_type='tcp';";

		return countCheck(sql);
	}

	/**
	 * Update the schema manager with new connection information.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @param databaseLocation 		The location of the local database. Used to determine whether a database in running in embedded mode.
	 * @return						Result of the update.
	 * @throws SQLException
	 */
	public int addLocalConnectionInformation(String localMachineAddress, int localMachinePort, String databaseLocation) throws SQLException{

		String connection_type = (localMachinePort == -1 && databaseLocation.contains("mem"))? "mem": "tcp";

		String sql = "\nINSERT INTO " + CONNECTIONS + " VALUES (null, '" + connection_type + "', '" + localMachineAddress + "', "  + localMachinePort + ");\n";

		return executeUpdate(sql);
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
	public int getNumberofReplicas(String tableName, String schemaName) throws SQLException{
		String sql = "SELECT count(*) FROM " + REPLICAS + ", " + TABLES + " WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND" +
		" " + TABLES + ".table_id=" + REPLICAS + ".table_id;";

		return getCount(sql);
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
	public int getConnectionID(String machine_name, int connection_port, String connection_type){
		if (cacheConnectionID != -1)
			return cacheConnectionID;

		String sql = "SELECT connection_id FROM " + CONNECTIONS + " WHERE machine_name='" + machine_name
		+ "' AND connection_port=" + connection_port + " AND connection_type='" + connection_type + "';";

		LocalResult result = null;
		try {
			sqlQuery = queryParser.prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(1);


			if (result.next()){
				cacheConnectionID = result.currentRow()[0].getInt();
				return cacheConnectionID;
			} else {
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
	public int getTableID(String tableName, String schemaName) throws SQLException{

		String sql = "SELECT table_id FROM " + TABLES + " WHERE tablename='" + tableName
		+ "' AND schemaname='" + schemaName + "';";

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
	public boolean isTableListed(String tableName, String schemaName) throws SQLException{
		String sql =  "SELECT count(*) FROM " + TABLES + " WHERE tablename='" + tableName + "' AND schemaname='" + schemaName +"';";

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
		String sql = "SELECT count(*) FROM " + REPLICAS + ", " + TABLES + " WHERE tablename='" + tableName + "' AND schemaname='" + 
		schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id AND db_location='"
		+ dbLocation + "' AND connection_id=" + connectionID + ";";

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
	private int addReplicaInformation(int tableID, long modificationID, int connectionID, String databaseLocation, String tableType, int tableSet) throws SQLException{
		String sql = "INSERT INTO " + REPLICAS + " VALUES (null, " + tableID + ", " + connectionID + ", '" + databaseLocation + "', '" + 
		tableType + "', " + modificationID +", " + tableSet + ");\n";
		return executeUpdate(sql);
	}

	/**
	 * Removes a table completely from the schema manager. Information is removed for the table itself and for all replicas.
	 * @param tableName		Leave null if you want to drop the entire schema.
	 * @param schemaName 
	 * @throws SQLException 
	 */
	public int removeTable(String tableName, String schemaName) throws SQLException {
		String sql = "";
		if (tableName == null){
			Integer[] tableIDs = getTableIDs(schemaName);

			sql = "DELETE FROM " + REPLICAS;
			for (int i = 0; i < tableIDs.length; i++){
				if (i==0){
					sql +=" WHERE table_id=" + tableIDs[i];
				} else {
					sql +=" OR table_id=" + tableIDs[i];
				}
			}
			sql += ";\nDELETE FROM " + TABLES + " WHERE schemaname='" + schemaName + "'; ";
		} else {

			int tableID = getTableID(tableName, schemaName);

			sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + ";";

			sql += "\nDELETE FROM " + TABLES + " WHERE table_id=" + tableID + "; ";

		}
		return executeUpdate(sql);
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
		sqlQuery = queryParser.prepareCommand(query);
		return sqlQuery.executeUpdate();
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
	public int removeReplica(String tableName, String dbLocation, String machineName, int connectionPort, String connectionType, String schemaName) throws SQLException {
		int connectionID = getConnectionID(machineName, connectionPort, connectionType);
		int tableID = getTableID(tableName, schemaName);
		String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + " AND db_location='" + dbLocation + "' AND connection_id=" + connectionID  + "; ";

		return executeUpdate(sql);
	}

	/**
	 * Get the names of all the tables in a given schema.
	 * @param schemaName
	 * @return
	 * @throws SQLException 
	 */
	public String[] getAllTablesInSchema(String schemaName) throws SQLException {
		String sql = "SELECT tablename FROM " + TABLES + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(0);

		Set<String> tableNames = new HashSet<String>();
		while (result.next()){
			tableNames.add(result.currentRow()[0].getString());
		}

		return tableNames.toArray(new String[0]);
	}

	/**
	 * Returns the next available tableSet number.
	 * @return
	 * @throws SQLException 
	 */
	public int getNewTableSetNumber() throws SQLException {
//		Random rnd = new Random();
//		int val = rnd.nextInt(100);
		String sql = "SELECT (max(table_set)+1) FROM " + REPLICAS + ";";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()){
			return result.currentRow()[0].getInt();
		} else {
			throw new SQLException("Internal problem: tableID not found in schema manager.");
		}

	}


}
