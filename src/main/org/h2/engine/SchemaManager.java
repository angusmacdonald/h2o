package org.h2.engine;

import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.result.LocalResult;

/**
 * Contains various utility methods which the system can use to access and modify H2O's schema manager. SQL for the tables is as follows:<code>
		CREATE SCHEMA IF NOT EXISTS H20;<br/>

		CREATE TABLE IF NOT EXISTS H20.H2O_TABLE(
		    tablename VARCHAR(255), 
			last_modification INT NOT NULL,
			PRIMARY KEY (tablename)
		);<br/>

		CREATE TABLE IF NOT EXISTS H20.H2O_REPLICA(
		    replica_id INT NOT NULL auto_increment,
			tablename VARCHAR(255), 
			connection_id INT NOT NULL,
			db_location VARCHAR(255),
			storage_type VARCHAR(50),
			last_modification INT NOT NULL,
			PRIMARY KEY (replica_id),
			FOREIGN KEY (tablename) REFERENCES H20.H2O_TABLE (tablename),
			FOREIGN KEY (connection_id) REFERENCES H20.H2O_CONNECTION (connection_id)
		);<br/>

		CREATE TABLE IF NOT EXISTS H20.H2O_CONNECTION(
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
	private static final String SCHEMA = "H20.";

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
			String localMachineAddress, int localMachinePort, String connection_type) throws SQLException {

		if (!isTableListed(tableName)){ // the table doesn't already exist in the schema manager.
			addTableInformation(tableName,  modificationID);
		}

		int connectionID = SchemaManager.getInstance().getConnectionID(localMachineAddress, localMachinePort, connection_type);

		if (!isReplicaListed(tableName, connectionID)){ // the table doesn't already exist in the schema manager.
			addReplicaInformation(tableName, modificationID, connectionID, databaseLocation, tableType);				
		}
	}

	/**
	 * Update the schema manager with new connection information.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @return						Result of the update.
	 * @throws SQLException
	 */
	public int addLocalConnectionInformation(String localMachineAddress, int localMachinePort) throws SQLException{
		String sql = "\nINSERT INTO " + CONNECTIONS + " VALUES (null, 'tcp', '" + localMachineAddress + "', "  + localMachinePort + ");\n";

		return executeUpdate(sql);
	}

	/**
	 * Creates the set of tables used by the schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public int createSchemaManagerTables() throws SQLException{
		String sql = "CREATE SCHEMA IF NOT EXISTS H20; " +
		"\n\nCREATE TABLE IF NOT EXISTS " + TABLES + "(tablename VARCHAR(255), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (tablename) );";

		sql += "CREATE TABLE IF NOT EXISTS " + CONNECTIONS +"(" + 
		"connection_id INT NOT NULL auto_increment," + 
		"connection_type VARCHAR(5), " + 
		"machine_name VARCHAR(255)," + 
		"connection_port INT NOT NULL, " + 
		"PRIMARY KEY (connection_id) );";

		sql += "\n\nCREATE TABLE IF NOT EXISTS " + REPLICAS + "(replica_id INT NOT NULL auto_increment, " +
		"tablename VARCHAR(255), " +
		"connection_id INT NOT NULL, " + 
		"db_location VARCHAR(255)," +
		"storage_type VARCHAR(50), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (tablename) REFERENCES " + TABLES + " (tablename) , " +
		" FOREIGN KEY (connection_id) REFERENCES " + CONNECTIONS + " (connection_id));\n";

		return executeUpdate(sql);
	}

	/**
	 * Creates linked tables for a remote schema manager, location specified by the paramter.
	 * @param schemaManagerLocation Location of the remote schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public int createLinkedTablesForSchemaManager(String schemaManagerLocation) throws SQLException{
		String sql = "DROP SCHEMA IF EXISTS H20; CREATE SCHEMA IF NOT EXISTS H20;";
		String tableName = TABLES;
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";
		tableName = CONNECTIONS;
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";
		tableName = REPLICAS;
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";


		return executeUpdate(sql);
	}

	/**
	 * Contacts the schema manager and accesses information on the set of available remote tables.
	 * @param localMachineAddress	The address of the local requesting machine (so as to exclude local results)
	 * @param localMachinePort	The port number of the local requesting machine (so as to exclude local results)
	 * @return	Result-set of all remote tables.
	 * @throws SQLException
	 */
	public LocalResult getAllRemoteTables(String localMachineAddress, int localMachinePort) throws SQLException{
		String sql = "SELECT tablename, db_location, connection_type, machine_name, connection_port " +
		"FROM " + REPLICAS + ", " + CONNECTIONS +
		" WHERE " + CONNECTIONS + ".connection_id = " + REPLICAS + ".connection_id " +
		"AND NOT (machine_name = '" + localMachineAddress + "' AND connection_port = " + localMachinePort + ");";

		return executeQuery(sql);
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
	 * A check whether a table is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @return	true, if the table's information is already in the schema manager.
	 * @throws SQLException 
	 */
	public boolean isTableListed(String tableName) throws SQLException{
		String sql =  "SELECT count(tablename) FROM " + TABLES + " WHERE tablename='" + tableName + "';";

		return countCheck(sql);
	}

	/**
	 * A check whether a replica is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param connectionID		Connection ID of the machine holding this replica.
	 * @return	true, if the replica's information is already in the schema manager.
	 * @throws SQLException 
	 */
	public boolean isReplicaListed(String tableName, int connectionID) throws SQLException{
		String sql = "SELECT count(tablename) FROM " + REPLICAS + " WHERE tablename='" + tableName + "' AND connection_id=" + connectionID + ";";

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
	 * Update the schema manager with new table information
	 * @param tableName			Name of the table being added.
	 * @param modificationID	Mofification ID of the table.
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addTableInformation(String tableName, long modificationID) throws SQLException{
		String sql = "INSERT INTO " + TABLES + " VALUES ('" + tableName + "', " + modificationID +");";
		return executeUpdate(sql);
	}

	/**
	 * Update the schema manager with new replica information
	 * @param tableName			Name of the replica being added.
	 * @param modificationID	Mofification ID of the table.
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addReplicaInformation(String tableName, long modificationID, int connectionID, String databaseLocation, String tableType) throws SQLException{
		String sql = "INSERT INTO " + REPLICAS + " VALUES (null, '" + tableName + "', " + connectionID + ", '" + databaseLocation + "', '" + 
		tableType + "', " + modificationID +");\n";
		return executeUpdate(sql);
	}
	
	/**
	 * Removes a table completely from the schema manager. Information is removed for the table itself and for all replicas.
	 * @param tableName
	 * @throws SQLException 
	 */
	public int removeTable(String tableName) throws SQLException {
		String sql = "DELETE FROM " + REPLICAS + " WHERE tablename='" + tableName + "'; ";
		
		sql += "\nDELETE FROM " + TABLES + " WHERE tablename='" + tableName + "';";
				
		return executeUpdate(sql);
	}
	

	private LocalResult executeQuery(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);

		return sqlQuery.executeQueryLocal(0);
	}

	private int executeUpdate(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);
		return sqlQuery.executeUpdate();
	}


}
