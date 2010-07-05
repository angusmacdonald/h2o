package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.autonomic.Settings;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LocalH2OProperties;
import org.h2.h2o.util.TableInfo;
import org.h2.h2o.util.locator.H2OLocatorInterface;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public abstract class PersistentManager {

	private ReplicaManager stateReplicaManager;

	/**
	 * Query parser instance to be used for all queries to the System Table.
	 */
	protected Parser queryParser;

	/**
	 * Command used as the container for all queries taking place in this instance.
	 */
	protected Command sqlQuery;
	private Database db;

	private String tableRelation;
	private String replicaRelation;
	private String connectionRelation;
	private String tableManagerRelation;

	private int managerStateReplicationFactor;

	private boolean metaDataReplicationEnabled;

	public PersistentManager(Database db, String tables, String replicas, String connections, String tableManagerRelation, int managerStateReplicationFactor) throws Exception{
		this.tableRelation = tables;
		this.replicaRelation = replicas;
		this.connectionRelation = connections;
		this.tableManagerRelation = tableManagerRelation;
		this.db = db;

		this.managerStateReplicationFactor = managerStateReplicationFactor;
		Session session = db.getSystemSession();

		metaDataReplicationEnabled = Boolean.parseBoolean(db.getDatabaseSettings().get("METADATA_REPLICATION_ENABLED"));
		
		if (session == null){
			ErrorHandling.error("Couldn't find system session. Local database has been shutdown.");
			return;
		}

		queryParser = new Parser(session, true);

		this.stateReplicaManager = new ReplicaManager();

		stateReplicaManager.add(db.getLocalDatabaseInstanceInWrapper());

		//stateReplicaManager.add(db.getLocalDatabaseInstance());

	}

	/**
	 * @param tableRelation
	 * @param replicaRelation
	 * @param connectionRelation
	 * @return
	 */
	public static String createSQL(String tables, String connections) {
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

		return sql;
	}

	/**
	 * Add a new table to the System Table. Called at the end of a CreateTable update. 
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
	public boolean addTableInformation(DatabaseURL tableManagerURL, TableInfo tableDetails, boolean addReplicaInfo) throws RemoteException, MovedException, SQLException{

		getNewQueryParser();

		try {
			assert !tableDetails.getTableName().startsWith("H2O_");

			DatabaseURL dbURL = tableDetails.getDbURL();

			if (dbURL == null){
				//find the URL from the Table Manager.
				dbURL = tableManagerURL;
			}

			int connectionID = getConnectionID(dbURL);

			assert connectionID != -1;

			if (!isTableListed(tableDetails)){ // the table doesn't already exist in the System Table.
				addTableInformation(tableDetails, connectionID);
			}

			if (addReplicaInfo){
				int tableID = getTableID(tableDetails);
				if (!isReplicaListed(tableDetails, connectionID)){ // the table doesn't already exist in the manager.
					addReplicaInformation(tableDetails, tableID, connectionID);				
				}
			}

			return true;
		} catch (SQLException e) {

			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Add a new replica to the System Table. The table already exists, so it is assumed there is an entry for that table
	 * in the System Table. This method only updates the replica table in the System Table.
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

			if (!isReplicaListed(tableDetails, connectionID)){ // the table doesn't already exist in the System Table.
				addReplicaInformation(tableDetails, tableID, connectionID);				
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Check if the System Table contains connection information for this database.
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
	 * Update the System Table with new connection information.
	 * @param localMachineAddress	The address through which remote machines can connect to the database.
	 * @param localMachinePort		The port on which the database is running.
	 * @param databaseLocation 		The location of the local database. Used to determine whether a database in running in embedded mode.
	 * @return						Result of the update.
	 */
	public int addConnectionInformation(DatabaseURL dbURL, boolean isActive) throws SQLException{

		//System.err.println("Adding connection info on " + db.getDatabaseURL().getURLwithRMIPort() + ". Info being added: "+ dbURL.getURLwithRMIPort());
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

		//System.err.println(sql);
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
		Session s = db.getSystemSession();
		queryParser = new Parser(s, true);

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
	public int getTableID(TableInfo ti) throws SQLException{

		String sql = "SELECT table_id FROM " + tableRelation + " WHERE tablename='" + ti.getTableName()
		+ "' AND schemaname='" + ti.getSchemaName() + "';";

		LocalResult result = null;

		sqlQuery = queryParser.prepareCommand(sql);

		result = sqlQuery.executeQueryLocal(1);

		if (result.next()){
			return result.currentRow()[0].getInt();
		} else {
			throw new SQLException("Internal problem: tableID not found in System Table.");
		}


	}


	/**
	 * A check whether a table is already listed in the System Table.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param schemaName 
	 * @return	true, if the table's information is already in the System Table.
	 * @throws SQLException 
	 */
	public boolean isTableListed(TableInfo ti) throws SQLException{
		String sql =  "SELECT count(*) FROM " + tableRelation + " WHERE tablename='" + ti.getTableName() + "' AND schemaname='" + ti.getSchemaName() +"';";

		return countCheck(sql);
	}

	/**
	 * A check whether a replica is already listed in the System Table.
	 * @param tableName			Name of the table for which the check is being made.
	 * @param connectionID		Connection ID of the machine holding this replica.
	 * @return	true, if the replica's information is already in the System Table.
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
	 * Update the System Table with new table information
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

	protected int addTableManagerReplicaInformation(int tableID, int connectionID, boolean active) throws SQLException{
		String sql = "INSERT INTO " + tableManagerRelation + " VALUES (" + tableID + ", " + connectionID + ", " + active + ");";
		return executeUpdate(sql);
	}

	protected int removeTableManagerReplicaInformation(int tableID, int connectionID) throws SQLException {
		String sql = "DELETE FROM " + tableManagerRelation + " WHERE table_id=" + tableID + " AND connection_id=" + connectionID + ";";
		return executeUpdate(sql);
	}

	/**
	 * Update the System Table with new replica information
	 * @param tableID			Name of the replica being added.
	 * @param modificationID	Mofification ID of the table.
	 * @return					Result of the update.
	 * @throws SQLException 
	 */
	private int addReplicaInformation(TableInfo ti, int tableID, int connectionID) throws SQLException{

		String sql = "INSERT INTO " + replicaRelation + " VALUES (null, " + tableID + ", " + connectionID + ", '" + 
		ti.getTableType() + "', " + ti.getModificationID() +", " + ti.getTableSet() + ");\n";
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
		//	getNewQueryParser();

		Set<DatabaseInstanceWrapper> replicas = stateReplicaManager.getActiveReplicas();
		Set<DatabaseInstanceWrapper> failed = new HashSet<DatabaseInstanceWrapper>();
		int result = -1;

		for (DatabaseInstanceWrapper replica: replicas){
			if (db.isLocal(replica)){
				sqlQuery = queryParser.prepareCommand(query);

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
					e.printStackTrace();
					failed.add(replica);

					if (this instanceof TableManager){
						//Remove table replica information from the system table.
						try {
							this.db.getSystemTable().removeTableManagerStateReplica(this.getTableInfo(), this.getLocation());
						} catch (RemoteException e1) {
							e1.printStackTrace();
						} catch (MovedException e1) {
							e1.printStackTrace();
						}
					}
				}
			}
		}

		boolean hasRemoved = replicas.removeAll(failed);

		if (hasRemoved){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Removed one or more replica locations because they couldn't be contacted for the last update.");
		}

		return result;
	}

	/**
	 * @return
	 * @throws MovedException 
	 */
	protected abstract DatabaseURL getLocation() throws RemoteException, MovedException;

	/**
	 * @return
	 */
	protected abstract TableInfo getTableInfo() throws RemoteException;

	/**
	 * Removes a particular replica from the System Table. 
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
	 * Removes a table completely from the System Table. Information is removed for the table itself and for all replicas.
	 * @param removeReplicaInfo Table managers need to remove replica information, system tables don't.
	 * @param tableName		Leave null if you want to drop the entire schema.
	 * @param schemaName 
	 * @throws SQLException 
	 */
	public boolean removeTableInformation(TableInfo ti, boolean removeReplicaInfo) {
		try {

			String sql = "";
			if (ti.getTableName() == null){
				/*
				 * Deleting the entire schema.
				 */
				Integer[] tableIDs;

				tableIDs = getTableIDs(ti.getSchemaName());


				if (tableIDs.length > 0 && removeReplicaInfo){
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

				sql = "";

				if (removeReplicaInfo){
					sql = "DELETE FROM " + replicaRelation + " WHERE table_id=" + tableID + ";";
				}
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
	protected boolean getNewQueryParser() {
		Session s = null;

		if (db.getSessions(false).length > 0){
			s = db.getSessions(false)[0];
		} else {
			s = db.getSystemSession();
		}

		if (s == null) return false;

		queryParser = new Parser(s, true);

		return true;

	}

	/**
	 * Add a location where replicas of this managers state will be placed.
	 * @param databaseWrapper
	 * @throws RemoteException
	 */
	public boolean addStateReplicaLocation(DatabaseInstanceWrapper databaseWrapper) throws RemoteException {
		
		if (metaDataReplicationEnabled){
			if (stateReplicaManager.size() < managerStateReplicationFactor + 1){ //+1 because the local copy counts as a replica.

				//now replica state here.
				try {
					String query = "DROP REPLICA IF EXISTS ";
					query += tableRelation + ", ";
					query += (replicaRelation == null)? "": replicaRelation + ", ";
					query += (tableManagerRelation == null)? "": tableManagerRelation + ", ";
					query += (connectionRelation == null)? "": connectionRelation + ";";
					databaseWrapper.getDatabaseInstance().executeUpdate(query, true);

					query = "CREATE REPLICA " + tableRelation + ", " + ((replicaRelation==null)? "": (replicaRelation + ", ")) + 
					connectionRelation + ((tableManagerRelation == null)? "": (", " + tableManagerRelation)) + " FROM '" + db.getURL().getOriginalURL() + "';";
					databaseWrapper.getDatabaseInstance().executeUpdate(query, true);
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "H2O Schema Tables replicated on new successor node: " + databaseWrapper.getDatabaseURL().getDbLocation());

					stateReplicaManager.add(databaseWrapper);

					return true;
				} catch (SQLException e) {
					e.printStackTrace();
					ErrorHandling.errorNoEvent("Failed to replicate manager/table state onto: " + databaseWrapper.getDatabaseURL().getDbLocation());
				} catch (Exception e) {
					throw new RemoteException(e.getMessage());
				} 

			}
		}
		return false;
	}

	/**
	 * 
	 */
	protected void updateLocatorFiles() throws Exception{
		LocalH2OProperties persistedInstanceInformation = new LocalH2OProperties(db.getURL());
		persistedInstanceInformation.loadProperties();

		String descriptorLocation = persistedInstanceInformation.getProperty("descriptor");
		String databaseName = persistedInstanceInformation.getProperty("databaseName");

		if (descriptorLocation == null || databaseName == null){
			throw new Exception("The location of the database descriptor must be specifed (it was not found). The database will now terminate.");
		}
		H2OLocatorInterface dl = new H2OLocatorInterface(databaseName, descriptorLocation);

		boolean successful = dl.setLocations(stateReplicaManager.getReplicaLocations());

		if (!successful){
			ErrorHandling.errorNoEvent("Didn't successfully write new System Replica locations to a majority of locator servers.");
		}
	}


	public void removeConnectionInformation(
			DatabaseInstanceRemote databaseInstance)
	throws RemoteException, MovedException {

		/*
		 * If the System Tables state is replicateed onto this machine remove it as a replica location.
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
	 * Change the location of the Table Manager to the new location specified.
	 * @param tableInfo 	New location of the Table Manager.
	 */
	public void changeTableManagerLocation(TableInfo tableInfo) {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "About to update the location of the Table Manager " + tableInfo + ".");

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
