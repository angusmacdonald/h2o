package org.h2.h2o.manager;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.autonomic.AutonomicAction;
import org.h2.h2o.autonomic.AutonomicController;
import org.h2.h2o.autonomic.Replication;
import org.h2.h2o.autonomic.Updates;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.locking.ILockingTable;
import org.h2.h2o.locking.LockingTable;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TableInfo;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

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
public class DataManager implements DataManagerRemote, AutonomicController, Migratable {

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

	private ReplicaManager replicaManager;

	/**
	 * Updates made asynchronously to a single table that haven't yet reached other replicas.
	 * 
	 * <p>Key: The number given to the update by the data manager.
	 * <p>Value: The SQL query for the update.
	 */
	private Map<Integer, String> unPropagatedUpdates;
	private Map<Integer, String> inProgressUpdates;

	/**
	 * Stores locks held by various databases for accessing this table (all replicas).
	 */
	private ILockingTable lockingTable;

	private Database database;

	private boolean isAlive = true;

	private boolean shutdown = false;

	/*
	 * MIGRATION RELATED CODE.
	 */
	/**
	 * If this schema manager has been moved to another location (i.e. its state has been transferred to another machine
	 * and it is no longer active) this field will not be null, and will note the new location of the schema manager.
	 */

	private String movedLocation = null;

	/**
	 * Whether the schema manager is in the process of being migrated. If this is true the schema manager will be 'locked', unable to service requests.
	 */
	private boolean inMigration;

	/**
	 * Whether the schema manager has been moved to another location.
	 */
	private boolean hasMoved = false;

	/**
	 * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
	 */
	private long migrationTime = 0l;

	/**
	 * The timeout period for migrating the schema manager.
	 */
	private static final int MIGRATION_TIMEOUT = 10000;

	private IChordRemoteReference location;

	public DataManager(String tableName, String schemaName, long modificationID, int tableSet, Database database) throws SQLException{
		this.tableName = tableName;

		if (schemaName.equals("") || schemaName == null){
			schemaName = "PUBLIC";
		}

		this.schemaName = schemaName;

		this.cachedTableID = -1;

		this.queryParser = new Parser(database.getSystemSession(), true);

		this.replicaManager = new ReplicaManager();
		this.unPropagatedUpdates = new HashMap<Integer, String>();
		this.inProgressUpdates = new HashMap<Integer, String>();

		this.lockingTable = new LockingTable(schemaName + "." + tableName);

		this.location = database.getChordInterface().getLocalChordReference();
		//this.primaryLocation = database.getLocalDatabaseInstance();

		//		this.primaryLocation = getDatabaseInstance(createFullDatabaseLocation(database.getDatabaseLocation(), 
		//				database.getConnectionType(), database.getLocalMachineAddress(), database.getLocalMachinePort() + "", database.isSM()));
		//	this.replicaLocations.add(primaryLocation);

		this.database = database;

		addInformationToDB(modificationID, database.getDatabaseLocation(), "TABLE", database.getLocalMachineAddress(), 
				database.getLocalMachinePort(), database.getConnectionType(), tableSet, database.getSchemaManagerReference().isSchemaManagerLocal());

		//database.addDataManager(this);
	}

	/**
	 * Creates a new DataManager object from the state of a data manager which is no longer running, but which has
	 * been persisted to disk.
	 * @param schemaName
	 * @param tableName
	 * @return
	 */
	public static DataManager createDataManagerFromPersistentStore(String schemaName, String tableName){
		//TODO unimplemented.

		return null;

	}

	/**
	 * Creates the set of tables used by the data manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public static int createDataManagerTables(Session session) throws SQLException{

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Creating data manager tables.");

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
		try {
			return query.update();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#addReplicaInformation(int, java.lang.String, java.lang.String, java.lang.String, int, java.lang.String, int)
	 */
	public boolean addReplicaInformation(long modificationID,
			String databaseLocation, String tableType,
			String localMachineAddress, int localMachinePort, String connectionType, int replicaSet, boolean isSM) throws RemoteException, MovedException{
		preMethodTest();

		if (!tableName.startsWith("H2O_")){

			try {

				int connectionID = getConnectionID(localMachineAddress, localMachinePort, connectionType);
				int tableID;

				tableID = getTableID();

				DatabaseURL databaseURL = createFullDatabaseLocation(databaseLocation, connectionType, localMachineAddress, localMachinePort + "", isSM);

				if (!isReplicaListed(connectionID, databaseLocation)){ // the table doesn't already exist in the schema manager.
					int result = addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, replicaSet, false);

					replicaManager.add(getDatabaseInstance(databaseURL));

					return (result == 1);
				} else {
					replicaManager.add(getDatabaseInstance(databaseURL));

					return false;
				}
			} catch (SQLException e) {
				e.printStackTrace();

				System.err.println("Error occured adding replica information.");
				return false;
			}
		} else {
			return false;
		}
	}

	/**
	 * @param replicaLocationString
	 * @return
	 */
	private DatabaseInstanceRemote getDatabaseInstance(
			DatabaseURL dbURL) {
		DatabaseInstanceRemote dir = database.getDatabaseInstance(dbURL);

		if (dir == null){
			try {
				//The schema manager doesn't contain a proper reference for the remote database instance. Try and find one,
				//then update the schema manager if successful.
				dir = database.getRemoteInterface().getDatabaseInstanceAt(dbURL);

				if (dir == null){
					ErrorHandling.errorNoEvent("DatabaseInstanceRemote wasn't found.");
				} else {

					database.getSchemaManager().addConnectionInformation(dbURL, new DatabaseInstanceWrapper(dir, true));

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}



		return dir;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.IDataManager#getQueryProxy(java.lang.String)
	 */
	@Override
	public synchronized QueryProxy getQueryProxy(LockType lockRequested, DatabaseInstanceRemote databaseInstanceRemote) throws RemoteException, SQLException, MovedException {
		preMethodTest();

		//if (!isAlive ) return null;


		if (replicaManager.size() == 0){
			try {
				throw new Exception("Illegal State. There must be at least one replica");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		LockType lockGranted = lockingTable.requestLock(lockRequested, databaseInstanceRemote);

		//		if (lockGranted == LockType.NONE){
		//			throw new SQLException("Table already locked. Cannot perform query.");
		//		}

		QueryProxy qp = new QueryProxy(lockGranted, schemaName + "." + tableName, selectReplicaLocations(replicaManager.getPrimary(), lockRequested, databaseInstanceRemote), 
				this, databaseInstanceRemote, replicaManager.getNewUpdateID(), lockRequested);

		return qp;
	}

	//	private String getPrimaryReplicaLocation() throws SQLException{
	//		if (cachedReplicaLocation != null)
	//			return cachedReplicaLocation;
	//
	//		String sql = "SELECT db_location, connection_type, machine_name, connection_port " +
	//		"FROM H2O.H2O_REPLICA, H2O.H2O_CONNECTION, H2O.H2O_TABLE " +
	//		"WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND " + TABLES + ".table_id=" + REPLICAS + ".table_id " + 
	//		"AND H2O_CONNECTION.connection_id = H2O_REPLICA.connection_id AND primary_copy = true;";
	//
	//		LocalResult result = null;
	//
	//		sqlQuery = queryParser.prepareCommand(sql);
	//
	//		try {
	//
	//			result = sqlQuery.executeQueryLocal(1);
	//
	//		} catch (Exception e){
	//			e.printStackTrace();
	//		}
	//
	//		if (result != null && result.next()){
	//			Value[] row = result.currentRow();
	//
	//			String dbLocation = row[0].getString();
	//			String connectionType = row[1].getString();
	//			String machineName = row[2].getString();
	//			String connectionPort = row[3].getString();
	//
	//			String dbName = null;
	//			if (connectionType.equals("tcp")){
	//				dbName = "jdbc:h2:" + connectionType + "://" + machineName + ":" + connectionPort + "/" + dbLocation;
	//			} else if (connectionType.equals("mem")){
	//				dbName = "jdbc:h2:" + dbLocation;
	//			} else {
	//				Message.throwInternalError("This connection type isn't supported yet. Get on that!");
	//			}
	//
	//			cachedReplicaLocation = dbName;
	//			return cachedReplicaLocation;
	//		}
	//
	//		throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
	//
	//
	//	}

	/**
	 * <p>Selects a set of replica locations on which replicas will be created for a given table or schema.
	 * 
	 * <p>This decision is currently based on the DESIRED_REPLICATION_FACTOR variable (if the query is a create), the SYNCHRONOUS_UPDATE variable
	 * if the query is another form of update, and the database instance where the request was initiated.
	 * @param primaryLocation	The location of the primary copy - also the location of the data manager. This location will NOT
	 * 	be returned in the list of replica locations (because the primary copy already exists there).
	 * @param lockType 
	 * @param databaseInstanceRemote Requesting machine.
	 * @return The set of database instances that should host a replica for the given table/schema. The return value will be NULL if
	 * 	no more replicas need to be created.
	 */
	private Set<DatabaseInstanceRemote> selectReplicaLocations(DatabaseInstanceRemote primaryLocation, LockType lockType, DatabaseInstanceRemote requestingDatabase) {
		/*
		 * The set of machines onto which new replicas will be added.
		 */
		Set<DatabaseInstanceRemote> newReplicaLocations = new HashSet<DatabaseInstanceRemote>();

		/*
		 * The set of all replica locations that could be involved in the query.
		 */
		Set<DatabaseInstanceWrapper> potentialReplicaLocations;

		if (lockType == LockType.CREATE){

			if (Replication.REPLICATION_FACTOR == 1){
				return null; //No more replicas are needed currently.
			}

			potentialReplicaLocations = database.getDatabaseInstances(); //the update could be sent to any or all machines in the system.

			int currentReplicationFactor = 1; //currently one copy of the table.

			/*
			 * Loop through all potential replica locations, selecting enough to satisfy the system's
			 * replication fact. The location of the primary copy cannot be re-used.
			 */
			for (DatabaseInstanceWrapper dbInstance: potentialReplicaLocations){
				if (!dbInstance.equals(primaryLocation)){ //primary copy doesn't exist here.
					newReplicaLocations.add(dbInstance.getDatabaseInstance());
					currentReplicationFactor++;
				}

				/*
				 * Do we have enough replicas yet?
				 */
				if (currentReplicationFactor == Replication.REPLICATION_FACTOR) break;
			}

			if (currentReplicationFactor < Replication.REPLICATION_FACTOR){
				//Couldn't replicate to enough machines.
				ErrorHandling.errorNoEvent("Insufficient number of machines available to reach a replication factor of " + Replication.REPLICATION_FACTOR);
			}


		} else if (lockType == LockType.WRITE){
			Set<DatabaseInstanceRemote> replicaLocations = this.replicaManager.getActiveReplicas(); //The update could be sent to any or all machines holding the given table.

			if (Updates.SYNCHRONOUS_UPDATE){
				//Update must be sent to all replicas:
				return replicaLocations;
			} else {
				//Update should only be sent to a single replica location. Choose that location.
				if (replicaLocations.contains(requestingDatabase)){
					newReplicaLocations.add(requestingDatabase); //try to keep the request local.
				} else {
					//Just pick another machine.
					DatabaseInstanceRemote randomDir = replicaLocations.toArray(new DatabaseInstanceRemote[0])[0];
					//TODO there has to be a better way of choosing this.
					newReplicaLocations.add(randomDir);
				}
			}
		}

		return newReplicaLocations;
	}

	public int removeReplica(String dbLocation, String machineName, int connectionPort, String connectionType) throws RemoteException, SQLException, MovedException {
		preMethodTest();

		int connectionID = getConnectionID(machineName, connectionPort, connectionType);
		int tableID = getTableID();

		String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + " AND db_location='" + dbLocation + "' AND connection_id=" + connectionID  + "; ";

		//Don't know if it is on a SM machine, so try both.
		//TODO push this logic down somewhere else.
		DatabaseInstanceRemote dbInstance = database.getDatabaseInstance(createFullDatabaseLocation(dbLocation, connectionType, machineName, connectionPort + "", false));
		if (dbInstance == null){
			dbInstance =  database.getDatabaseInstance(createFullDatabaseLocation(dbLocation, connectionType, machineName, connectionPort + "", true));
			if (dbInstance == null){
				ErrorHandling.errorNoEvent("Couldn't remove replica location.");
			}
		}

		replicaManager.remove(dbInstance);

		return executeUpdate(sql);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#removeDataManager()
	 */
	@Override
	public int removeDataManager() throws RemoteException, SQLException, MovedException {	
		preMethodTest();

		int tableID = getTableID();
		String sql = "DELETE FROM " + REPLICAS + " WHERE table_id=" + tableID + ";\nDELETE FROM " + TABLES + " WHERE tablename='" + tableName
		+ "' AND schemaname='" + schemaName + "';";

		//replicaLocations.removeAll(null);

		//		if (true){// Diagnostic.getLevel() == DiagnosticLevel.FULL
		//			/*
		//			 * Test that the row is there as expected.
		//			 */
		//			String testQuery = "SELECT * FROM " + TABLES + " WHERE tablename='" + tableName
		//			+ "' AND schemaname='" + schemaName + "';";
		//			
		//			LocalResult result = executeQuery(testQuery);
		//			
		//			//assert result.getRowCount() == 1;
		//		
		//			if (result.getRowCount() != 1){
		//				
		//				
		//				testQuery = "SELECT * FROM " + TABLES + ";";
		//				
		//				result = executeQuery(testQuery);
		//				System.out.println("result");
		//				assert false;
		//			}
		//		}

		return executeUpdate(sql);
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#getLocation()
	 */
	@Override
	public String getLocation() throws RemoteException, MovedException{
		preMethodTest();

		return replicaManager.getPrimary().getConnectionString();
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

		if (!tableName.startsWith("H2O_")){

			if (!isTableListed()){ // the table doesn't already exist in the schema manager.
				addTableInformation(modificationID);
			}

			int connectionID = getConnectionID(localMachineAddress, localMachinePort, connectionType);
			int tableID = getTableID();
			if (!isReplicaListed(connectionID, databaseLocation)){ // the table doesn't already exist in the schema manager.
				addReplicaInformation(tableID, modificationID, connectionID, databaseLocation, tableType, tableSet, true);
			}
		}
		replicaManager.add(getDatabaseInstance(createFullDatabaseLocation(databaseLocation, connectionType, localMachineAddress, localMachinePort + "", isSM)));
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
		int result = sqlQuery.executeUpdate(true);

		return result;
	}

	private DatabaseURL createFullDatabaseLocation(String dbLocationOnDisk, String connectionType, String machineName, String connectionPort, boolean isSM){
		DatabaseURL dbURL = new DatabaseURL(connectionType, machineName, Integer.parseInt(connectionPort), dbLocationOnDisk, isSM);

		return dbURL;
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

		String sql = "SELECT PRIMARY table_id FROM " + TABLES + " WHERE tablename='" + tableName
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
		return tableName;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.DataManagerRemote#testAvailability()
	 */
	@Override
	public boolean isAlive() throws RemoteException, MovedException {
		preMethodTest();
		
		return true;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#releaseLock(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void releaseLock(DatabaseInstanceRemote requestingDatabase, Set<DatabaseInstanceRemote> updatedReplicas, int updateID) throws RemoteException, MovedException {
		preMethodTest();

		/*
		 * Update the set of 'active replicas' and their update IDs. 
		 */
		replicaManager.completeUpdate(updatedReplicas, updateID);

		/*
		 * Release the locks.
		 */
		lockingTable.releaseLock(requestingDatabase);

	}

	/**
	 * @return
	 * @throws MovedException 
	 * @throws RemoteException 
	 */
	public TableInfo getTableInfo() throws RemoteException {

		return new TableInfo(tableName, schemaName, database.getDatabaseURL());
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#shutdown()
	 */
	@Override
	public void shutdown() {
		isAlive = false;
	}

	/*******************************************************
	 * Methods implementing the Migrate interface.
	 ***********************************************************/

	private void preMethodTest() throws RemoteException, MovedException{
		if (hasMoved || shutdown){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Data manager has moved. Throwing MovedException.");
			throw new MovedException(movedLocation);
		}
		/*
		 * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period, throw an execption. 
		 */
		if (inMigration){
			//If it hasn't moved, but is in the process of migration an exception will be thrown.
			long currentTimeOfMigration = System.currentTimeMillis() - migrationTime;

			if (currentTimeOfMigration < MIGRATION_TIMEOUT) {
				throw new RemoteException();
			} else {
				inMigration = false; //Timeout request.
				this.migrationTime = 0l;
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#checkConnection()
	 */
	@Override
	public void checkConnection() throws RemoteException, MovedException {
		preMethodTest();

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#completeMigration()
	 */
	@Override
	public void completeMigration() throws RemoteException, MovedException,
	MigrationException {
		if (!inMigration){ // the migration process has timed out.
			throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
		}

		this.hasMoved = true;
		this.inMigration = false;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#prepareForMigration(java.lang.String)
	 */
	@Override
	public void prepareForMigration(String newLocation) throws RemoteException,
	MigrationException, MovedException {
		preMethodTest();

		movedLocation = newLocation;

		inMigration = true;

		migrationTime = System.currentTimeMillis();
	}


	public void buildDataManagerState(DataManagerRemote otherDataManager) throws RemoteException, MovedException {
		preMethodTest();

		/*
		 * Obtain fully qualified table name.
		 */
		//		this.schemaName = otherDataManager.getSchemaName();
		//		this.tableName = otherDataManager.getTableName();
		//This is done when constructing the new data manager.

		/*
		 * Obtain replica manager.
		 */
		this.replicaManager = otherDataManager.getReplicaManager();


	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#getSchemaName()
	 */
	@Override
	public String getSchemaName()  throws RemoteException {
		return schemaName;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#getReplicaManager()
	 */
	@Override
	public ReplicaManager getReplicaManager() throws RemoteException {
		return this.replicaManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#getTableSet()
	 */
	@Override
	public int getTableSet()  throws RemoteException {
		return 1; //TODO implement
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.DataManagerRemote#getDatabaseURL()
	 */
	@Override
	public DatabaseURL getDatabaseURL() throws RemoteException {
		return database.getDatabaseURL();
	}


	/*******************************************************
	 * Methods implementing the AutonomicController interface.
	 ***********************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.autonomic.AutonomicController#changeSetting(org.h2.h2o.autonomic.AutonomicAction)
	 */
	@Override
	public boolean changeSetting(AutonomicAction action) throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean shutdown) throws RemoteException,
	MovedException {
		this.shutdown = shutdown;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#getChordReference()
	 */
	@Override
	public IChordRemoteReference getChordReference() throws RemoteException {
		return location;
	}

}
