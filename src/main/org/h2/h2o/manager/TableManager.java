package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashSet;
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
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.locking.ILockingTable;
import org.h2.h2o.locking.LockingTable;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TableInfo;

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
	private static final String TABLES = SCHEMA + "H2O_DM_TABLE";

	/**
	 * Name of replicas' table in Table Manager.
	 */
	private static final String REPLICAS = SCHEMA + "H2O_DM_REPLICA";

	/**
	 * Name of connections' table in Table Manager.
	 */
	private static final String CONNECTIONS = SCHEMA + "H2O_DM_CONNECTION";

	/**
	 * The database username used to communicate with Table Manager tables.
	 */
	public static String USERNAME = "angus";

	/**
	 * The database password used to communicate with Table Manager tables.
	 */
	public static String PASSWORD = "supersecret";

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

	public TableManager(TableInfo tableDetails, Database database) throws Exception{
		super(database, false, TABLES, REPLICAS, CONNECTIONS);

		this.tableName = tableDetails.getTableName();

		this.schemaName = tableDetails.getSchemaName();

		if (schemaName.equals("") || schemaName == null){
			schemaName = "PUBLIC";
		}

		this.replicaManager = new ReplicaManager();
		//		this.unPropagatedUpdates = new HashMap<Integer, String>();
		//		this.inProgressUpdates = new HashMap<Integer, String>();

		this.lockingTable = new LockingTable(schemaName + "." + tableName);

		this.location = database.getChordInterface().getLocalChordReference();
		//this.primaryLocation = database.getLocalDatabaseInstance();

		//		this.primaryLocation = getDatabaseInstance(createFullDatabaseLocation(database.getDatabaseLocation(), 
		//				database.getConnectionType(), database.getLocalMachineAddress(), database.getLocalMachinePort() + "", database.isSM()));
		//	this.replicaLocations.add(primaryLocation);


		addTableInformation(getDB().getDatabaseURL(), tableDetails);

		//database.addTableManager(this);
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
		
		added = super.addTableInformation(tableManagerURL, tableDetails, true);
		if (added) replicaManager.add(getDatabaseInstance(tableManagerURL));
		return added;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#addReplicaInformation(org.h2.h2o.util.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo tableDetails)throws RemoteException, MovedException, SQLException {
		super.addConnectionInformation(tableDetails.getDbURL(), true);
		super.addReplicaInformation(tableDetails);
		replicaManager.add(getDatabaseInstance(tableDetails.getDbURL()));
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#removeReplicaInformation(org.h2.h2o.util.TableInfo)
	 */
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException{
		super.removeReplicaInformation(ti);

		DatabaseInstanceRemote dbInstance = getDB().getDatabaseInstance(ti.getDbURL());
		if (dbInstance == null){
			dbInstance =  getDB().getDatabaseInstance(ti.getDbURL());
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
	public boolean removeTableManager() throws RemoteException, SQLException,
			MovedException {
		boolean successful = super.removeTableInformation(getTableInfo());
		
		return successful;
	}

	/**
	 * Creates the set of tables used by the Table Manager.
	 * @return Result of the update.
	 * @throws SQLException
	 */
	public static int createTableManagerTables(Session session) throws SQLException{

		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Creating Table Manager tables.");

		String sql = createSQL(TABLES, REPLICAS, CONNECTIONS);


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

	/**
	 * @param replicaLocationString
	 * @return
	 */
	private DatabaseInstanceRemote getDatabaseInstance(DatabaseURL dbURL) {
		DatabaseInstanceRemote dir = getDB().getDatabaseInstance(dbURL);

		if (dir == null){
			try {
				//The System Table doesn't contain a proper reference for the remote database instance. Try and find one,
				//then update the System Table if successful.
				dir = getDB().getRemoteInterface().getDatabaseInstanceAt(dbURL);

				if (dir == null){
					ErrorHandling.errorNoEvent("DatabaseInstanceRemote wasn't found.");
				} else {

					getDB().getSystemTable().addConnectionInformation(dbURL, new DatabaseInstanceWrapper(dir, true));

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}



		return dir;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.ITableManager#getQueryProxy(java.lang.String)
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getQueryProxy(org.h2.h2o.util.LockType, org.h2.h2o.comms.remote.DatabaseInstanceRemote)
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

			potentialReplicaLocations = getDB().getDatabaseInstances(); //the update could be sent to any or all machines in the system.

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



	/* (non-Javadoc)
	 * @see org.h2.h2o.comms.TableManagerRemote#getLocation()
	 */
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getLocation()
	 */
	@Override
	public String getLocation() throws RemoteException, MovedException{
		preMethodTest();

		return getDB().getDatabaseURL().getOriginalURL();
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

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#getTableInfo()
	 */
	public TableInfo getTableInfo() throws RemoteException {

		return new TableInfo(tableName, schemaName, getDB().getDatabaseURL());
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


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.TableManagerRemote2#buildTableManagerState(org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	public void buildTableManagerState(TableManagerRemote otherTableManager) throws RemoteException, MovedException {
		preMethodTest();

		/*
		 * Obtain fully qualified table name.
		 */
		//		this.schemaName = otherTableManager.getSchemaName();
		//		this.tableName = otherTableManager.getTableName();
		//This is done when constructing the new Table Manager.

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
	public ReplicaManager getReplicaManager() throws RemoteException {
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
		return getDB().getDatabaseURL();
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

}
