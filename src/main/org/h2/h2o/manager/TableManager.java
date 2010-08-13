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
package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.h2o.autonomic.AutonomicAction;
import org.h2.h2o.autonomic.AutonomicController;
import org.h2.h2o.autonomic.Updates;
import org.h2.h2o.autonomic.decision.RequestType;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.locking.ILockingTable;
import org.h2.h2o.locking.LockingTable;
import org.h2.h2o.remote.StartupException;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TableInfo;
import org.h2.h2o.util.event.DatabaseStates;
import org.h2.h2o.util.event.H2OEvent;
import org.h2.h2o.util.event.H2OEventBus;
import org.h2.result.LocalResult;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * <p>The Table Manager represents a user table in H2O, and is responsible for storing
 * information on replicas for that table, and handing out locks to access those replicas.</p>
 * 
 * <p>There is one Table Manager for every user table in the system.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableManager extends PersistentManager implements TableManagerRemote, AutonomicController, Migratable {

	/**
	 * Name of the schema used to store Table Manager tables.
	 */
	private static final String SCHEMA = "H2O.";

	/**
	 * Name of tables' table in Table Manager.
	 */
	public static final String TABLES = "H2O_TM_TABLE";

	/**
	 * Name of replicas' table in Table Manager.
	 */
	public static final String REPLICAS = "H2O_TM_REPLICA";

	/**
	 * Name of connections' table in Table Manager.
	 */
	public static final String CONNECTIONS = "H2O_TM_CONNECTION";

	public static final String TABLEMANAGERSTATE = "H2O_TM_TABLEMANAGERS";

	/**
	 * The name of the table that this Table Manager is responsible for.
	 */
	private String tableName;

	/**
	 * Name of the schema in which the table is located.
	 */
	private String schemaName;

	private ReplicaManager replicaManager;

	//	/**
	//	 * Updates made asynchronously to a single table that haven't yet reached other replicas.
	//	 * 
	//	 * <p>Key: The number given to the update by the Table Manager.
	//	 * <p>Value: The SQL query for the update.
	//	 */
	//	private Map<Integer, String> unPropagatedUpdates;
	//	private Map<Integer, String> inProgressUpdates;

	/**
	 * Stores locks held by various databases for accessing this table (all replicas).
	 */
	private ILockingTable lockingTable;

	private boolean shutdown = false;

	/*
	 * MIGRATION RELATED CODE.
	 */
	/**
	 * If this System Table has been moved to another location (i.e. its state has been transferred to another machine
	 * and it is no longer active) this field will not be null, and will note the new location of the System Table.
	 */

	private String movedLocation = null;

	/**
	 * Whether the System Table is in the process of being migrated. If this is true the System Table will be 'locked', unable to service requests.
	 */
	private boolean inMigration;

	/**
	 * Whether the System Table has been moved to another location.
	 */
	private boolean hasMoved = false;

	/**
	 * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
	 */
	private long migrationTime = 0l;

	/**
	 * The timeout period for migrating the System Table.
	 */
	private static final int MIGRATION_TIMEOUT = 10000;


	private IChordRemoteReference location;

	private String fullName;

	private int relationReplicationFactor;

	public TableManager(TableInfo tableDetails, Database database) throws Exception{
		super(database);

		String dbName = database.getURL().sanitizedLocation();
		setMetaDataTableNames(getMetaTableName(dbName, TABLES), getMetaTableName(dbName, REPLICAS), getMetaTableName(dbName, CONNECTIONS), getMetaTableName(dbName, TABLEMANAGERSTATE));


		this.tableName = tableDetails.getTableName();

		this.schemaName = tableDetails.getSchemaName();

		if (schemaName.equals("") || schemaName == null){
			schemaName = "PUBLIC";
		}

		this.fullName = schemaName + "." + tableName;

		this.replicaManager = new ReplicaManager();
		this.replicaManager.add(database.getLocalDatabaseInstanceInWrapper()); //the first replica will be created here.

		this.lockingTable = new LockingTable(schemaName + "." + tableName);

		this.location = database.getChordInterface().getLocalChordReference();

		this.relationReplicationFactor = Integer.parseInt(database.getDatabaseSettings().get("RELATION_REPLICATION_FACTOR"));
		
		H2OEventBus.publish(new H2OEvent(database.getURL(), DatabaseStates.TABLE_MANAGER_CREATION, tableDetails.getFullTableName()));
	}

	public static String getMetaTableName (String databaseName, String tablePostfix){
		return SCHEMA + "H2O_" + databaseName + "_" + tablePostfix;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#addTableInformation(org.h2.h2o.util.DatabaseURL, org.h2.h2o.util.TableInfo)
	 */
	@Override
	public boolean addTableInformation(DatabaseURL tableManagerURL,
			TableInfo tableDetails) throws RemoteException, MovedException, SQLException {
		int result = super.addConnectionInformation(tableManagerURL, true);

		boolean added = (result != -1);
		if (!added) return false;

		return super.addTableInformation(tableManagerURL, tableDetails, true);

		/*
		 * The System Table isn't contacted here, but in the Create Table class. This is because the Table isn't officially
		 * created until the end of CreateTable.update().
		 */
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#addReplicaInformation(org.h2.h2o.util.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo tableDetails)throws RemoteException, MovedException, SQLException {
		preMethodTest();

		super.addConnectionInformation(tableDetails.getURL(), true);
		super.addReplicaInformation(tableDetails);
		replicaManager.add(getDatabaseInstance(tableDetails.getURL()));
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#removeReplicaInformation(org.h2.h2o.util.TableInfo)
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException{
		super.removeReplicaInformation(ti);

		DatabaseInstanceRemote dbInstance = getDB().getDatabaseInstance(ti.getURL());
		if (dbInstance == null){
			dbInstance =  getDB().getDatabaseInstance(ti.getURL());
			if (dbInstance == null){
				ErrorHandling.errorNoEvent("Couldn't remove replica location.");
			}
		}

		replicaManager.remove(dbInstance);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#removeTableManager()
	 */
	@Override
	public boolean removeTableInformation() throws RemoteException, SQLException,
	MovedException {
		return removeTableInformation(getTableInfo(), true);
	}

	@Override
	public boolean removeTableInformation(TableInfo tableInfo, boolean removeReplicaInfo) {
		return super.removeTableInformation(getTableInfo(), removeReplicaInfo);
	}

	/**
	 * Creates the set of tables used by the Table Manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public static int createTableManagerTables(Session session) throws SQLException{

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Creating Table Manager tables.");

		String databaseName = session.getDatabase().getURL().sanitizedLocation().toUpperCase();
		String sql = createSQL(getMetaTableName(databaseName, TableManager.TABLES), getMetaTableName(databaseName, TableManager.CONNECTIONS));

		sql += "\n\nCREATE TABLE IF NOT EXISTS " + getMetaTableName(databaseName, TableManager.REPLICAS) + "(" +
		"replica_id INTEGER NOT NULL auto_increment(1,1), " +
		"table_id INTEGER NOT NULL, " +
		"connection_id INTEGER NOT NULL, " + 
		"storage_type VARCHAR(255), " + 
		"last_modification INT NOT NULL, " +
		"table_set INT NOT NULL, " +
		"PRIMARY KEY (replica_id), " +
		"FOREIGN KEY (table_id) REFERENCES " + getMetaTableName(databaseName, TableManager.TABLES) + " (table_id) ON DELETE CASCADE , " +
		" FOREIGN KEY (connection_id) REFERENCES " + getMetaTableName(databaseName, TableManager.CONNECTIONS) + " (connection_id));";


		Parser parser = new Parser(session, true);

		Command query = parser.prepareCommand(sql);
		try {
			return query.update();
		} catch (RemoteException e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * Get the database instance at the specified database URL.
	 * @param dbURL location of the database instance.
	 * @return null if the instance wasn't found (including if it wasn't active).
	 */
	private DatabaseInstanceWrapper getDatabaseInstance(DatabaseURL dbURL) {
		ISystemTable systemTable = getDB().getSystemTableReference().getSystemTable();

		DatabaseInstanceRemote dir = null;

		if (systemTable != null){
			try {
				dir = systemTable.getDatabaseInstance(dbURL);
			} catch (RemoteException e1) {
				e1.printStackTrace();
			} catch (MovedException e1) {
				try {
					getDB().getSystemTableReference().handleMovedException(e1);
					dir = systemTable.getDatabaseInstance(dbURL);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		if (dir == null){
			try {
				//The System Table doesn't contain a proper reference for the remote database instance. Try and find one,
				//then update the System Table if successful.
				dir = getDB().getRemoteInterface().getDatabaseInstanceAt(dbURL);

				if (dir == null){
					ErrorHandling.errorNoEvent("DatabaseInstanceRemote wasn't found.");
				} else {

					getDB().getSystemTable().addConnectionInformation(dbURL, new DatabaseInstanceWrapper(dbURL, dir, true));

				}

			} catch (Exception e) {
				//e.printStackTrace();
			}
		}



		return new DatabaseInstanceWrapper(dbURL, dir, true);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.ITableManager#getQueryProxy(java.lang.String)
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getQueryProxy(org.h2.h2o.util.LockType, org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public synchronized QueryProxy getQueryProxy(LockType lockRequested, DatabaseInstanceWrapper databaseInstanceWrapper) throws RemoteException, SQLException, MovedException {
		preMethodTest();

		//if (!isAlive ) return null;


		if (replicaManager.size() == 0 && !lockRequested.equals(LockType.CREATE)){
			try {
				throw new Exception("Illegal State. There must be at least one replica");
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		LockType lockGranted = lockingTable.requestLock(lockRequested, databaseInstanceWrapper);

		QueryProxy qp = new QueryProxy(lockGranted, fullName, selectReplicaLocations(lockRequested, databaseInstanceWrapper), 
				this, databaseInstanceWrapper, replicaManager.getNewUpdateID(), lockRequested);

		return qp;
	}

	/**
	 * <p>Selects a set of replica locations on which replicas will be created for a given table or schema.
	 * 
	 * <p>This decision is currently based on the DESIRED_REPLICATION_FACTOR variable (if the query is a create), the SYNCHRONOUS_UPDATE variable
	 * if the query is another form of update, and the database instance where the request was initiated.
	 * @param primaryLocation	The location of the primary copy - also the location of the Table Manager. This location will NOT
	 * 	be returned in the list of replica locations (because the primary copy already exists there).
	 * @param lockType 
	 * @param databaseInstanceRemote Requesting machine.
	 * @return The set of database instances that should host a replica for the given table/schema. The return value will be NULL if
	 * 	no more replicas need to be created.
	 */
	private Set<DatabaseInstanceWrapper> selectReplicaLocations(LockType lockType, DatabaseInstanceWrapper requestingDatabase) {

		if (lockType == LockType.READ){
			return this.replicaManager.getActiveReplicas();
		}// else, a more informed decision is needed.

		/*
		 * The set of machines onto which new replicas will be added.
		 */
		Set<DatabaseInstanceWrapper> newReplicaLocations = new HashSet<DatabaseInstanceWrapper>();

		/*
		 * The set of all replica locations that could be involved in the query.
		 */
		Queue<DatabaseInstanceWrapper> potentialReplicaLocations = null;

		if (lockType == LockType.CREATE){
			/*
			 * We know that the CREATE operation has been executed on the machine on which this Table
			 * Manager has been created, because it is the create operation that initializes the Table
			 * Manager in the first place. 
			 */
			newReplicaLocations.add(requestingDatabase);
			if (relationReplicationFactor == 1){		
				return newReplicaLocations; //No more replicas are needed currently.
			}

			try {
				potentialReplicaLocations = getDB().getSystemTable().getAvailableMachines(RequestType.CREATE_TABLE_QUERY); //the update could be sent to any or all machines in the system.
			} catch (RemoteException e) {
				e.printStackTrace();
			} catch (MovedException e) {
				e.printStackTrace();
			}

			int currentReplicationFactor = 1; //currently one copy of the table.

			/*
			 * Loop through all potential replica locations, selecting enough to satisfy the system's
			 * replication fact. The location of the primary copy cannot be re-used.
			 */

			if (potentialReplicaLocations != null && potentialReplicaLocations.size() > 0){

				for (DatabaseInstanceWrapper dbInstance: potentialReplicaLocations){
					//This includes the location of the primary copy.
					boolean added = newReplicaLocations.add(dbInstance);
					if (added) currentReplicationFactor++;

					/*
					 * Do we have enough replicas yet?
					 */
					if (currentReplicationFactor == relationReplicationFactor) break;
				}

			}

			if (currentReplicationFactor < relationReplicationFactor){
				//Couldn't replicate to enough machines.
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Insufficient number of machines available to reach a replication factor of " + relationReplicationFactor
						+ ". The table will be replicated on " + currentReplicationFactor + " instances.");
			}


		} else if (lockType == LockType.WRITE){
			Set<DatabaseInstanceWrapper> replicaLocations = this.replicaManager.getActiveReplicas(); //The update could be sent to any or all machines holding the given table.

			if (Updates.SYNCHRONOUS_UPDATE){
				//Update must be sent to all replicas:
				return replicaLocations;
			} else {
				//Update should only be sent to a single replica location. Choose that location.
				if (replicaLocations.contains(requestingDatabase)){
					newReplicaLocations.add(requestingDatabase); //try to keep the request local.
				} else {
					//Just pick another machine.
					DatabaseInstanceWrapper randomDir = replicaLocations.toArray(new DatabaseInstanceWrapper[0])[0];
					//TODO there has to be a better way of choosing this.
					newReplicaLocations.add(randomDir);
				}
			}
		}

		return newReplicaLocations;
	}



	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.TableManagerRemote#getLocation()
	 */
	@Override
	public DatabaseURL getLocation() throws RemoteException, MovedException{
		preMethodTest();

		return getDB().getURL();
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getTableName()
	 */
	public String getTableName() {
		return tableName;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.TableManagerRemote#testAvailability()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#isAlive()
	 */
	@Override
	public boolean isAlive() throws RemoteException, MovedException {
		if (shutdown) return false;

		preMethodTest();

		return true;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#releaseLock(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#releaseLock(org.h2.h2o.comms.remote.DatabaseInstanceRemote, java.util.Set, int)
	 */
	@Override
	public void releaseLock(DatabaseInstanceWrapper requestingDatabase, Set<DatabaseInstanceWrapper> updatedReplicas, int updateID) throws RemoteException, MovedException {
		preMethodTest();

		/*
		 * Update the set of 'active replicas' and their update IDs. 
		 */
		replicaManager.completeUpdate(updatedReplicas, updateID, true);

		/*
		 * Release the locks.
		 */
		lockingTable.releaseLock(requestingDatabase);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getTableInfo()
	 */
	public TableInfo getTableInfo() {

		return new TableInfo(tableName, schemaName, getDB().getURL());
	}

	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#shutdown()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#shutdown()
	 */
	@Override
	public void shutdown() {
		//		isAlive = false;
	}

	/*******************************************************
	 * Methods implementing the Migrate interface.
	 ***********************************************************/

	private void preMethodTest() throws RemoteException, MovedException{
		if (hasMoved || shutdown){
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Table Manager has moved. Throwing MovedException.");
			throw new MovedException(movedLocation);
		}
		/*
		 * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period, throw an execption. 
		 */
		if (inMigration){
			//If it hasn't moved, but is in the process of migration an exception will be thrown.
			long currentTimeOfMigration = System.currentTimeMillis() - migrationTime;

			if (currentTimeOfMigration > MIGRATION_TIMEOUT) {
				inMigration = false; //Timeout request.
				this.migrationTime = 0l;

				throw new RemoteException("Timeout exception. Migration took too long. Current time :" + currentTimeOfMigration + ", TIMEOUT time: " + MIGRATION_TIMEOUT);
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


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#buildTableManagerState(org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	public void buildTableManagerState(TableManagerRemote otherTableManager) throws RemoteException, MovedException {
		preMethodTest();

		/*
		 * Table name, schema name, and other infor are already obtained when the table manager instance is created.
		 */

		/*
		 * Obtain replica manager.
		 */
		this.replicaManager = otherTableManager.getReplicaManager();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#getSchemaName()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getSchemaName()
	 */
	@Override
	public String getSchemaName()  throws RemoteException {
		return schemaName;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#getReplicaManager()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getReplicaManager()
	 */
	@Override
	public ReplicaManager getReplicaManager() throws RemoteException, MovedException {
		preMethodTest();

		return this.replicaManager;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#getTableSet()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getTableSet()
	 */
	@Override
	public int getTableSet()  throws RemoteException {
		return 1; //TODO implement
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#getDatabaseURL()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getDatabaseURL()
	 */
	@Override
	public DatabaseURL getDatabaseURL() throws RemoteException {
		return getDB().getURL();
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
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean shutdown) throws RemoteException,
	MovedException {
		this.shutdown = shutdown;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#getChordReference()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getChordReference()
	 */
	@Override
	public IChordRemoteReference getChordReference() throws RemoteException {
		return location;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#recreateReplicaManagerState()
	 */
	@Override
	public void recreateReplicaManagerState(String oldPrimaryDatabaseName) throws RemoteException, SQLException {
		ReplicaManager rm = new ReplicaManager();

		/*
		 * Get Replica information from persisted state.
		 */

		String oldTableRelation = getMetaTableName(oldPrimaryDatabaseName, TABLES);
		String oldconnectionRelation = getMetaTableName(oldPrimaryDatabaseName, CONNECTIONS);
		String oldReplicaRelation = getMetaTableName(oldPrimaryDatabaseName, REPLICAS);

		String sql = "SELECT LOCAL ONLY connection_type, machine_name, db_location, connection_port, chord_port FROM " + oldReplicaRelation + ", " + oldTableRelation + ", " + oldconnectionRelation + 
		" WHERE tablename = '" + tableName + "' AND schemaname='" + schemaName + "' AND" +
		" " + oldTableRelation + ".table_id=" + oldReplicaRelation + ".table_id AND " + oldconnectionRelation + ".connection_id=" + oldReplicaRelation + ".connection_id;";

		LocalResult rs = null;
		try{
			rs = executeQuery(sql);
		} catch (SQLException e){
			System.err.println("tablename: " + tableName + ", old primary: " + oldPrimaryDatabaseName);
			throw e;
		}

		List<DatabaseInstanceWrapper> replicaLocations = new LinkedList<DatabaseInstanceWrapper>();
		while (rs.next()){

			DatabaseURL dbURL = new DatabaseURL(rs.currentRow()[0].getString(), rs.currentRow()[1].getString(), 
					rs.currentRow()[3].getInt(), rs.currentRow()[2].getString(), false, rs.currentRow()[4].getInt());


			//Don't include the URL of the old instance unless it is still running.
			DatabaseInstanceWrapper replicaLocation = getDatabaseInstance(dbURL);

			boolean alive = true;
			if(dbURL.sanitizedLocation().equals(oldPrimaryDatabaseName)) {
				try {
					alive = replicaLocation.getDatabaseInstance().isAlive();
				} catch (Exception e) {
					alive = false;
				}
			}

			replicaLocation.setActive(alive); //even dead replicas must be recorded.
			replicaLocations.add(replicaLocation);

		}

		if (replicaLocations.size() == 0){
			throw new SQLException("No replicas were listed for this table. An internal error has occured.");
		}

		rm.add(replicaLocations);

		this.replicaManager = rm;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#getNumberofReplicas()
	 */
	@Override
	public int getNumberofReplicas() throws RemoteException {
		return replicaManager.getNumberOfReplicas();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.remote.TableManagerRemote#persistToCompleteStartup()
	 */
	@Override
	public void persistToCompleteStartup(TableInfo tableInfo) throws RemoteException, StartupException {

		try {
			addTableInformation(getDB().getURL(), tableInfo);
		} catch (MovedException e) {
			throw new StartupException("Newly created Table Manager throws a MovedException. This should never happen - serious internal error.");
		} catch (SQLException e) {
			throw new StartupException("Failed to persist table manager meta-data to disk: " + e.getMessage());
		}

	}

	public void persistReplicaInformation() {
		for (DatabaseInstanceWrapper dir: replicaManager.getActiveReplicas()){
			TableInfo ti = new TableInfo(getTableInfo());
			ti.setURL(dir.getURL());
			try {
				super.addConnectionInformation(ti.getURL(), true);
				super.addReplicaInformation(ti);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MovedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
