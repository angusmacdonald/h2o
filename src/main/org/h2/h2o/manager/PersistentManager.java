package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.autonomic.Replication;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PersistentManager {

	private ReplicaManager stateReplicaManager;

	/**
	 * Query parser instance to be used for all queries to the schema manager.
	 */
	private Parser queryParser;

	/**
	 * Command used as the container for all queries taking place in this instance.
	 */
	protected Command sqlQuery;
	private Database db;

	private String tableRelation;
	private String replicaRelation;
	private String connectionRelation;

	public PersistentManager(Database db, boolean createTables, String tables, String replicas, String connections) throws Exception{
		this.tableRelation = tables;
		this.replicaRelation = replicas;
		this.connectionRelation = connections;
		this.db = db;

		Session session = db.getSystemSession();

		if (session == null){
			ErrorHandling.error("Couldn't find system session. Local database has been shutdown.");
			db.getSchemaManager().stopLookupPinger();
			return;
		}

		queryParser = new Parser(session, true);

		this.stateReplicaManager = new ReplicaManager();

		stateReplicaManager.add(db.getLocalDatabaseInstance());

		if (createTables){
			/*
			 * Create a new set of schema tables locally.
			 */
			try {
				setupManagerStateTables();
			} catch (SQLException e) {

				e.printStackTrace();
				throw new Exception("Couldn't create manager state tables.");
			}
		}

		stateReplicaManager.add(db.getLocalDatabaseInstance());

	}

	/**
	 * @param tableRelation
	 * @param replicaRelation
	 * @param connectionRelation
	 * @return
	 */
	public static String createSQL(String tables, String replicas, String connections) {
		String sql = "CREATE SCHEMA IF NOT EXISTS H2O; ";

		sql += "CREATE TABLE IF NOT EXISTS " + connections +"(" + 
		"connection_id INTEGER NOT NULL auto_increment(1,1)," + 
		"connection_type VARCHAR(5), " + 
		"machine_name VARCHAR(255)," + 
		"db_location VARCHAR(255)," +
		"connection_port INT NOT NULL, " + 
		"chord_port INT NOT NULL, " +
		"active BOOLEAN, " + 
		"PRIMARY KEY (connection_id) );";

		sql+="CREATE SCHEMA IF NOT EXISTS H2O; " +
		"\n\nCREATE TABLE IF NOT EXISTS " + tables + "( table_id INT NOT NULL auto_increment(1,1), " +
		"schemaname VARCHAR(255), tablename VARCHAR(255), " +
		"last_modification INT NOT NULL, " +
		"manager_location INTEGER NOT NULL, " +
		"PRIMARY KEY (table_id), " + 
		"FOREIGN KEY (manager_location) REFERENCES " + connections + " (connection_id));";


		sql += "\n\nCREATE TABLE IF NOT EXISTS " + replicas + "(" +
		"replica_id INTEGER NOT NULL auto_increment(1,1), " +
		"table_id INTEGER NOT NULL, " +
		"connection_id INTEGER NOT NULL, " + 
		"storage_type VARCHAR(255), " + 
		"last_modification INT NOT NULL, " +
		"table_set INT NOT NULL, " +
		"primary_copy BOOLEAN, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (table_id) REFERENCES " + tables + " (table_id) ON DELETE CASCADE , " +
		" FOREIGN KEY (connection_id) REFERENCES " + connections + " (connection_id));";
		return sql;
	}

	/**
	 * Creates the set of tables used by the schema manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	protected int setupManagerStateTables() throws SQLException{

		String sql = createSQL(tableRelation, replicaRelation, connectionRelation);

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

	/**
	 * Add a new table to the schema manager. Called at the end of a CreateTable update. 
	 * @param tableName				Name of the table being added.
	 * @param modificationId		Modification ID of the table.
	 * @param databaseLocation		Location of the table (the database it is stored in)
	 * @param tableType				Type of the table (e.g. Linked, View, Table).
	 * @param localMachineAddress	Address through which the DB is contactable.
	 * @param localMachinePort		Port the server is running on.
	 * @param connection_type		The type of connection (e.g. TCP, FTP).
	 * @throws MovedException 
	 * @throws RemoteException 
	 * @throws SQLException 
	 */
	public boolean addTableInformation(DatabaseURL dataManagerURL, TableInfo tableDetails) throws RemoteException, MovedException, SQLException{

		getNewQueryParser();

		try {
			String tableName = tableDetails.getTableName();
			String schemaName = tableDetails.getSchemaName();

			assert !tableName.startsWith("H2O_");




			DatabaseURL dbURL = tableDetails.getDbURL();

			if (dbURL == null){
				//find the URL from the data manager.
				dbURL = dataManagerURL;
			}

			int connectionID = getConnectionID(dbURL);

			assert connectionID != -1;

			if (!isTableListed(tableDetails)){ // the table doesn't already exist in the schema manager.
				addTableInformation(tableDetails, connectionID);
			}


			int tableID = getTableID(tableDetails);
			if (!isReplicaListed(tableDetails, connectionID)){ // the table doesn't already exist in the schema manager.
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
	 * @param tableDetails		Fully qualified name of the table, and its location (as a DatabaseURL).
	 * @throws RemoteException	Thrown if there was a problem connnecting to this instance.
	 * @throws MovedException	Thrown if the instance has been migrated to another machine.
	 * @throws SQLException
	 */
	public void addReplicaInformation(TableInfo tableDetails) throws RemoteException, MovedException, SQLException {
		//getNewQueryParser();

		//queryParser = new Parser(db.getExclusiveSession(), true);

		try {
			int connectionID = getConnectionID(tableDetails.getDbURL());
			int tableID = getTableID(tableDetails);

			if (!isReplicaListed(tableDetails, connectionID)){ // the table doesn't already exist in the schema manager.
				addReplicaInformation(tableDetails, tableID, connectionID, false);				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Check if the schema manager contains connection information for this database.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @return						True, if the connection information already exists.
	 * @throws SQLException
	 */
	public boolean connectionInformationExists(DatabaseURL dbURL) throws SQLException{
		String sql = "SELECT count(connection_id) FROM " + connectionRelation + " WHERE machine_name='" + dbURL.getHostname() + "' AND db_location='" +  dbURL.getDbLocation() +
		"' AND connection_port=" + dbURL.getPort() + " AND connection_type='" + dbURL.getConnectionType() +"';";

		return countCheck(sql);
	}

	/**
	 * Update the schema manager with new connection information.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @param databaseLocation 		The location of the local database. Used to determine whether a database in running in embedded mode.
	 * @return						Result of the update.
	 */
	public int addConnectionInformation(DatabaseURL dbURL, boolean isActive) throws SQLException{
		Session s = db.getSystemSession();
		queryParser = new Parser(s, true);

		String connection_type = dbURL.getConnectionType();

		String sql = null;

		if (connectionInformationExists(dbURL)){
			//Update existing information - the chord port may have changed.

			sql = "\nUPDATE " + connectionRelation + " SET chord_port = " + dbURL.getRMIPort() + 
			", active = " + isActive + " WHERE machine_name='" + dbURL.getHostname() + "' AND connection_port=" + dbURL.getPort() +
			" AND connection_type='" + dbURL.getConnectionType() +"';";

		} else { 
			//Create a new entry.
			sql = "\nINSERT INTO " + connectionRelation + " VALUES (null, '" + connection_type + "', '" + dbURL.getHostname() + 
			"', '" + dbURL.getDbLocation() + "', "  + dbURL.getPort() + ", " +
			dbURL.getRMIPort() + ", " + isActive + ");\n";

		}

		return executeUpdate(sql);

	}


	/**
	 * Gets the number of replicas that exist for a given table.
	 */
	public int getNumberofReplicas(String tableName, String schemaName){
		String sql = "SELECT count(*) FROM " + replicaRelation + ", " + tableRelation + " WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND" +
		" " + tableRelation + ".table_id=" + replicaRelation + ".table_id;";

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

		String sql = "SELECT connection_id FROM " + connectionRelation + " WHERE machine_name='" + machine_name
		+ "' AND connection_port=" + connection_port + " AND connection_type='" + connection_type + "' AND db_location = '" + dbURL.getDbLocation() + "';";

		LocalResult result = null;
		try {
			sqlQuery = queryParser.prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(1);

			if (result.next()){
				return result.currentRow()[0].getInt();
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

		String sql = "SELECT table_id FROM " + tableRelation + " WHERE tablename='" + ti.getTableName()
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
		String sql =  "SELECT count(*) FROM " + tableRelation + " WHERE tablename='" + ti.getTableName() + "' AND schemaname='" + ti.getSchemaName() +"';";

		return countCheck(sql);
	}

	/**
	 * A check whether a replica is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param connectionID		Connection ID of the machine holding this replica.
	 * @return	true, if the replica's information is already in the schema manager.
	 * @throws SQLException 
	 */
	public boolean isReplicaListed(TableInfo ti, int connectionID) throws SQLException{
		String sql = "SELECT count(*) FROM " + replicaRelation + ", " + tableRelation + ", " + connectionRelation + " WHERE tablename='" + ti.getTableName() + "' AND schemaname='" + 
		ti.getSchemaName() + "' AND " + tableRelation + ".table_id=" + replicaRelation + ".table_id AND " + replicaRelation + ".connection_id = " + connectionRelation + ".connection_id AND " +
		connectionRelation + ".connection_id = " + connectionID + ";";

		return countCheck(sql);
	}

	/**
	 * Takes in an SQL count(*) query, which should return a single result, which is a single integer, indicating
	 * the number of occurences of a given entry. If the number of entries is greater than zero true is returned; otherwise false.
	 * @param query	SQL count query.
	 * @return
	 * @throws SQLException
	 */
	protected boolean countCheck(String query) throws SQLException{
		return (getCount(query)>0);
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
	private int addTableInformation(TableInfo tableInfo, int connectionID) throws SQLException{
		String sql = "INSERT INTO " + tableRelation + " VALUES (null, '" + tableInfo.getSchemaName() + "', '" + tableInfo.getTableName() + "', " + tableInfo.getModificationID() + ", " +
		connectionID +");";
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
		String sql = "INSERT INTO " + replicaRelation + " VALUES (null, " + tableID + ", " + connectionID + ", '" + 
		ti.getTableType() + "', " + ti.getModificationID() +", " + ti.getTableSet() + ", " + primaryCopy + ");\n";
		return executeUpdate(sql);
	}

	/**
	 * Gets all the table IDs for tables in a schema.
	 * @param schemaName
	 * @return
	 * @throws SQLException 
	 */
	protected Integer[] getTableIDs(String schemaName) throws SQLException {
		String sql = "SELECT table_id FROM " + tableRelation + " WHERE schemaname='" + schemaName + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(0);

		Set<Integer> ids = new HashSet<Integer>();
		while (result.next()){
			ids.add(result.currentRow()[0].getInt());
		}

		return ids.toArray(new Integer[0]);
	}

	protected LocalResult executeQuery(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);

		return sqlQuery.executeQueryLocal(0);
	}

	protected int executeUpdate(String query) throws SQLException{
		//getNewQueryParser();

		Set<DatabaseInstanceRemote> replicas = stateReplicaManager.getActiveReplicas();

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
	 * @throws MovedException 
	 * @throws RemoteException 
	 * @throws SQLException 
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException {

		try {

			int connectionID = getConnectionID(ti.getDbURL());
			int tableID;

			tableID = getTableID(ti);

			String sql = "DELETE FROM " + replicaRelation + " WHERE table_id=" + tableID + " AND connection_id=" + connectionID  + "; ";

			executeUpdate(sql);

		} catch (SQLException e) {
			e.printStackTrace();
		}
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

				int tableID = getTableID(ti);

				sql = "DELETE FROM " + replicaRelation + " WHERE table_id=" + tableID + ";";

				sql += "\nDELETE FROM " + tableRelation + " WHERE table_id=" + tableID + "; ";

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
	 * Add a location where replicas of this managers state will be placed.
	 * @param databaseReference
	 * @throws RemoteException
	 */
	public void addStateReplicaLocation(
			DatabaseInstanceRemote databaseReference) throws RemoteException {

		if (stateReplicaManager.size() < Replication.SCHEMA_MANAGER_REPLICATION_FACTOR + 1){ //+1 because the local copy counts as a replica.

			//now replica state here.
			try {
				databaseReference.executeUpdate("DROP REPLICA IF EXISTS " + tableRelation + ", " + replicaRelation + ", " + connectionRelation + ";");
				databaseReference.executeUpdate("CREATE REPLICA " + tableRelation + ", " + replicaRelation + ", " + connectionRelation + " FROM '" + db.getDatabaseURL().getOriginalURL() + "';");
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O Schema Tables replicated on new successor node: " + databaseReference.getConnectionURL().getDbLocation());

				stateReplicaManager.add(databaseReference);

			} catch (SQLException e) {
				ErrorHandling.errorNoEvent("Failed to replicate schema manager state on: " + databaseReference.getConnectionURL().getDbLocation());
			} 



		}
	}


	public void removeConnectionInformation(
			DatabaseInstanceRemote databaseInstance)
	throws RemoteException, MovedException {

		/*
		 * If the schema managers state is replicateed onto this machine remove it as a replica location.
		 */
		this.stateReplicaManager.remove(databaseInstance);
		/*
		 * TODO try to replicate somewhere else.
		 */


		DatabaseURL dburl = databaseInstance.getConnectionURL(); 
		String sql = "\nUPDATE " + connectionRelation + " SET active = false WHERE machine_name='" + dburl.getHostname() + "' AND connection_port=" + dburl.getPort() +
		" AND connection_type='" + dburl.getConnectionType() +"';";

		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}


	}

	protected Database getDB(){
		return db;
	}

	/**
	 * @return
	 */
	protected Parser getParser() {
		return queryParser;
	}

	/**
	 * Change the location of the data manager to the new location specified.
	 * @param tableInfo 	New location of the data manager.
	 */
	public void changeDataManagerLocation(TableInfo tableInfo) {
		int connectionID = getConnectionID(tableInfo.getDbURL());

		assert connectionID != -1;

		String sql = "\nUPDATE " + tableRelation + " SET manager_location = " + connectionID + " WHERE schemaname='" + tableInfo.getSchemaName() + "' AND tablename='" + 
		tableInfo.getTableName() + "';";

		try {
			executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

}
