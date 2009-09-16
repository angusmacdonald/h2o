package org.h2.engine;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.h2o.comms.DataManagerRemote;
import org.h2.h2o.comms.DatabaseInstanceRemote;
import org.h2.h2o.comms.QueryProxy;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.test.db.Db;
import org.h2.value.Value;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * <p>The data manager represents a user table in H2O, and is responsible for storing
 * information on replicas for that table, and handing out locks to access those replicas.</p>
 * 
 * <p>There is one data manager for every user table in the system.
 * 
 * <p>Data manager state is stored in the database in the following tables
 	<code>
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
			primary_copy BOOLEAN,
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
public class DataManager implements DataManagerRemote {

	/**
	 * Name of the schema used to store data manager tables.
	 */
	private static final String SCHEMA = "H2O.";

	/**
	 * Name of tables' table in data manager.
	 */
	private static final String TABLES = SCHEMA + "H2O_DM_TABLE";

	/**
	 * Name of replicas' table in data manager.
	 */
	private static final String REPLICAS = SCHEMA + "H2O_DM_REPLICA";

	/**
	 * Name of connections' table in data manager.
	 */
	private static final String CONNECTIONS = SCHEMA + "H2O_DM_CONNECTION";

	/**
	 * The database username used to communicate with data manager tables.
	 */
	public static String USERNAME = "angus";

	/**
	 * The database password used to communicate with data manager tables.
	 */
	public static String PASSWORD = "supersecret";

	/**
	 * Query parser instance to be used for all queries to the data manager.
	 */
	private Parser queryParser;

	private Command sqlQuery;

	/**
	 * The name of the table that this data manager is responsible for.
	 */
	private String tableName;

	/**
	 * Name of the schema in which the table is located.
	 */
	private String schemaName;

	/**
	 * The tableID for this table in the data manager tables.
	 */
	private int cachedTableID;

	/**
	 * The location of the primary replica - the same location as this data manager.
	 */
	private String cachedReplicaLocation;

	private Set<DatabaseInstanceRemote> replicaLocations;
	
	private Database database;

	public DataManager(String tableName, String schemaName, Session session, long modificationID, int tableSet, Database database) throws SQLException{
		this.tableName = tableName;
		
		if (schemaName.equals("") || schemaName == null){
			schemaName = "PUBLIC";
		}

		this.schemaName = schemaName;

		this.cachedTableID = -1;
		this.cachedReplicaLocation = null;
		this.queryParser = new Parser(session, true);

		this.replicaLocations = new HashSet<DatabaseInstanceRemote>();
		this.database = database;
		
		addInformationToDB(modificationID, database.getDatabaseLocation(), "TABLE", database.getLocalMachineAddress(), 
				database.getLocalMachinePort(), database.getConnectionType(), tableSet, database.isSM());

		database.addDataManager(this);
	}

	/**
	 * Creates the set of tables used by the data manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public static int createDataManagerTables(Session session) throws SQLException{
		Diagnostic.traceNoEvent(Diagnostic.FINAL, "Creating data manager tables.");

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
		"primary_copy BOOLEAN, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (table_id) REFERENCES " + TABLES + " (table_id) ON DELETE CASCADE , " +
		" FOREIGN KEY (connection_id) REFERENCES " + CONNECTIONS + " (connection_id));\n";

		Parser parser = new Parser(session, true);

		Command query = parser.prepareCommand(sql);
		return query.executeUpdate();
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#addReplicaInformation(int, java.lang.String, java.lang.String, java.lang.String, int, java.lang.String, int)
	 */
	public boolean addReplicaInformation(long modificationID,
			String databaseLocation, String tableType,
			String localMachineAddress, int localMachinePort, String connectionType, int replicaSet, boolean isSM) throws RemoteException{

		try {

			int connectionID = getConnectionID(localMachineAddress, localMachinePort, connectionType);
			int tableID;

			tableID = getTableID();

			String replicaLocationString = createFullDatabaseLocation(databaseLocation, connectionType, localMachineAddress, localMachinePort + "", isSM);

			if (!isReplicaListed(connectionID, databaseLocation)){ // the table doesn't already exist in the schema manager.
				int result = addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, replicaSet, false);
		
				replicaLocations.add(getDatabaseInstance(replicaLocationString));
				
				return (result == 1);
			} else {
				replicaLocations.add(getDatabaseInstance(replicaLocationString));
				
				return false;
			}
		} catch (SQLException e) {
			System.err.println("Error occured adding replica information.");
			return false;
		}
	}

	/**
	 * @param replicaLocationString
	 * @return
	 */
	private DatabaseInstanceRemote getDatabaseInstance(
			String replicaLocationString) {
		return database.getDatabaseInstance(replicaLocationString);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.IDataManager#requestLock(java.lang.String)
	 */
	@Override
	public QueryProxy requestQueryProxy(QueryProxy.LockType lockType) throws RemoteException {
		

		if (replicaLocations.size() == 0){
			try {
				throw new Exception("Illegal State. There must be at least one replica");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		QueryProxy qp = new QueryProxy(lockType, tableName, replicaLocations);

		return qp;
	}

	private String getPrimaryReplicaLocation() throws SQLException{
		if (cachedReplicaLocation != null)
			return cachedReplicaLocation;

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

			String dbName = null;
			if (connectionType.equals("tcp")){
				dbName = "jdbc:h2:" + connectionType + "://" + machineName + ":" + connectionPort + "/" + dbLocation;
			} else if (connectionType.equals("mem")){
				dbName = "jdbc:h2:" + dbLocation;
			} else {
				Message.throwInternalError("This connection type isn't supported yet. Get on that!");
			}

			cachedReplicaLocation = dbName;
			return cachedReplicaLocation;
		}

		throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);


	}



	public int removeReplica(String dbLocation, String machineName, int connectionPort, String connectionType) throws RemoteException, SQLException {
		int connectionID = getConnectionID(machineName, connectionPort, connectionType);
		int tableID = getTableID();
		String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + " AND db_location='" + dbLocation + "' AND connection_id=" + connectionID  + "; ";

		//Don't know if it is on a SM machine, so try both.
		replicaLocations.remove(createFullDatabaseLocation(dbLocation, connectionType, machineName, connectionPort + "", false));
		replicaLocations.remove(createFullDatabaseLocation(dbLocation, connectionType, machineName, connectionPort + "", true));

		return executeUpdate(sql);
	}
	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#removeDataManager()
	 */
	@Override
	public int removeDataManager() throws RemoteException, SQLException {
		int tableID = getTableID();
		String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + "; DELETE FROM " + TABLES + " WHERE tablename='" + tableName
		+ "' AND schemaname='" + schemaName + "';";

		//replicaLocations.removeAll(null);
		
		return executeUpdate(sql);
	}
	
	/**
	 * Add the new table to the schema manager. Called at the end of a CreateTable update. 
	 * @param tableName				Name of the table being added.
	 * @param modificationId		Modification ID of the table.
	 * @param databaseLocation		Location of the table (the database it is stored in)
	 * @param tableType				Type of the table (e.g. Linked, View, Table).
	 * @param localMachineAddress	Address through which the DB is contactable.
	 * @param localMachinePort		Port the server is running on.
	 * @param connectionType		The type of connection (e.g. TCP, FTP).
	 * @param isSM					True if the database is a schema manager.
	 * @throws SQLException 
	 */
	private void addInformationToDB(long modificationID,
			String databaseLocation, String tableType,
			String localMachineAddress, int localMachinePort, String connectionType, int tableSet, boolean isSM) throws SQLException {

		if (!isTableListed()){ // the table doesn't already exist in the schema manager.
			addTableInformation(modificationID);
		}

		int connectionID = getConnectionID(localMachineAddress, localMachinePort, connectionType);
		int tableID = getTableID();
		if (!isReplicaListed(connectionID, databaseLocation)){ // the table doesn't already exist in the schema manager.
			addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, tableSet, true);
		}
		
		replicaLocations.add(getDatabaseInstance(createFullDatabaseLocation(databaseLocation, connectionType, localMachineAddress, localMachinePort + "", isSM)));
	
	}

	/**
	 * Update the schema manager with new table information
	 * @param tableName			Name of the table being added.
	 * @param modificationID	Mofification ID of the table.
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addTableInformation(long modificationID) throws SQLException{
		String sql = "INSERT INTO " + TABLES + " VALUES (null, '" + schemaName + "', '" + tableName + "', " + modificationID +");";
		return executeUpdate(sql);
	}


	private int addReplicaInformation(int tableID, long modificationID, int connectionID, String databaseLocation, String tableType, int tableSet, boolean primaryCopy) throws SQLException{
		String sql = "INSERT INTO " + REPLICAS + " VALUES (null, " + tableID + ", " + connectionID + ", '" + databaseLocation + "', '" + 
		tableType + "', " + modificationID +", " + tableSet + ", " + primaryCopy + ");\n";

		return executeUpdate(sql);
	}

	/**
	 * A check whether a table is already listed in the schema manager.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param schemaName 
	 * @return	true, if the table's information is already in the schema manager.
	 * @throws SQLException 
	 */
	private boolean isTableListed() throws SQLException{
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
	private boolean isReplicaListed(int connectionID, String dbLocation) throws SQLException{
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



	private LocalResult executeQuery(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);

		return sqlQuery.executeQueryLocal(0);
	}

	private int executeUpdate(String query) throws SQLException{
		sqlQuery = queryParser.prepareCommand(query);
		return sqlQuery.executeUpdate();
	}

	private Set<DatabaseInstanceRemote> getAllReplicaLocations() throws SQLException{
		return replicaLocations;
	}

	private String createFullDatabaseLocation(String dbLocationOnDisk, String connectionType, String machineName, String connectionPort, boolean isSM){
		String dbName = null;
		if (connectionType.equals("tcp")){
			dbName = "jdbc:h2:" + ((isSM)? "sm:": "") + connectionType + "://" + machineName + ":" + connectionPort + "/" + dbLocationOnDisk;
		} else if (connectionType.equals("mem")){
			dbName = "jdbc:h2:"  + ((isSM)? "sm:": "") + dbLocationOnDisk;
		} else {
			Message.throwInternalError("This connection type isn't supported yet. Get on that!");
		}

		return dbName;
	}


	/**
	 * Gets the connection ID for a given database connection if it is present in the database. If not, -1 is returned.
	 * 
	 * If a connection ID for a given connection is not found then a new connection will be added to the DB.
	 * 
	 * @param machine_name
	 * @param connection_port
	 * @param connection_type
	 * @return
	 */
	private int getConnectionID(String machine_name, int connection_port, String connection_type){

		String sql = "SELECT connection_id FROM " + CONNECTIONS + " WHERE machine_name='" + machine_name
		+ "' AND connection_port=" + connection_port + " AND connection_type='" + connection_type + "';";

		LocalResult result = null;
		try {
			sqlQuery = queryParser.prepareCommand(sql);

			result = sqlQuery.executeQueryLocal(1);



			if (result.next()){
				return result.currentRow()[0].getInt();
			} else {
				sql = "\nINSERT INTO " + CONNECTIONS + " VALUES (null, '" + connection_type + "', '" + machine_name + "', "  + connection_port + ");\n";
				executeUpdate(sql);
				return getConnectionID(machine_name, connection_port, connection_type);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 *  Gets the table ID for a given database table if it is present in the database. If not, an exception is thrown.
	 * @param tableName		Name of the table
	 * @param schemaName	Schema the table is in.
	 * @return
	 */
	private int getTableID() throws SQLException{

		if (cachedTableID != -1)
			return cachedTableID;

		String sql = "SELECT table_id FROM " + TABLES + " WHERE tablename='" + tableName
		+ "' AND schemaname='" + schemaName + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()){
			cachedTableID = result.currentRow()[0].getInt();
			return cachedTableID;
		} else {
			throw new SQLException("Internal problem: tableID not found in schema manager.");
		}


	}

	/**
	 * @return
	 */
	public String getTableName() {
		return schemaName + "." + tableName;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#testAvailability()
	 */
	@Override
	public void testAvailability() {
		//Doesn't do anything.
	}

	



}
