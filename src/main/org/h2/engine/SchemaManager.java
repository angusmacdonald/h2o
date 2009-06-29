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

	Parser queryParser;
	Command sqlQuery;

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

	public static SchemaManager getInstance(Session session){
		if (singleton == null){
			singleton = new SchemaManager(session);
		}

		return singleton;
	}

	private SchemaManager(Session session) {
		queryParser = new Parser(session);
	}

	public int addLocalConnectionInformation(String localMachineAddress, int localMachinePort) throws SQLException{
		String sql = "\nINSERT INTO H20.H2O_CONNECTION VALUES (null, 'tcp', '" + localMachineAddress + "', "  + localMachinePort + ");\n";

		return executeUpdate(sql);
	}
	
	public int createSchemaManagerTables() throws SQLException{
		String sql = "CREATE SCHEMA IF NOT EXISTS H20; " +
		"\n\nCREATE TABLE IF NOT EXISTS H20.H2O_TABLE(tablename VARCHAR(255), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (tablename) );";

		sql += "CREATE TABLE IF NOT EXISTS H20.H2O_CONNECTION(" + 
		"connection_id INT NOT NULL auto_increment," + 
		"connection_type VARCHAR(5), " + 
		"machine_name VARCHAR(255)," + 
		"connection_port INT NOT NULL, " + 
		"PRIMARY KEY (connection_id) );";

		sql += "\n\nCREATE TABLE IF NOT EXISTS H20.H2O_REPLICA(replica_id INT NOT NULL auto_increment, " +
		"tablename VARCHAR(255), " +
		"connection_id INT NOT NULL, " + 
		"db_location VARCHAR(255)," +
		"storage_type VARCHAR(50), " +
		"last_modification INT NOT NULL, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (tablename) REFERENCES H20.H2O_TABLE (tablename) , " +
		" FOREIGN KEY (connection_id) REFERENCES H20.H2O_CONNECTION (connection_id));\n";
		
		return executeUpdate(sql);
	}
	
	public int createLinkedTablesForSchemaManager(String schemaManagerLocation) throws SQLException{
		String sql = "DROP SCHEMA IF EXISTS H20; CREATE SCHEMA IF NOT EXISTS H20;";
		String tableName = "H20.H2O_TABLE";
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";
		tableName = "H20.H2O_CONNECTION";
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";
		tableName = "H20.H2O_REPLICA";
		sql += "\nDROP TABLE IF EXISTS " + tableName + ";\nCREATE LINKED TABLE " + tableName + "('org.h2.Driver', '" + schemaManagerLocation + "', 'angus', 'supersecret', '" + tableName + "');";

		
		return executeUpdate(sql);
	}
	
	public LocalResult getAllRemoteTables(String localMachineAddress, int localMachinePort) throws SQLException{
		String sql = "SELECT tablename, db_location, connection_type, machine_name, connection_port " +
		"FROM H20.H2O_REPLICA, H20.H2O_CONNECTION " +
		"WHERE H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id " +
		"AND NOT (machine_name = '" + localMachineAddress + "' AND connection_port = " + localMachinePort + ");";

		return executeQuery(sql);
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
	 * Gets the connection ID for a given database connection if it is present in the database. If not, -1 is returned.
	 * @param machine_name
	 * @param connection_port
	 * @param connection_type
	 * @return
	 */
	public int getConnectionID(String machine_name, int connection_port, String connection_type){
		String sql = "SELECT connection_id FROM H20.H2O_CONNECTION WHERE machine_name='" + machine_name
		+ "' AND connection_port=" + connection_port + " AND connection_type='" + connection_type + "';";

		LocalResult result = null;
		try {
			sqlQuery = queryParser.prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(1);

			
			if (result.next()){
				return result.currentRow()[0].getInt();
			} else {
				return -1;
			}
			

		} catch (SQLException e) {
			return -1;
		}
	}
}
