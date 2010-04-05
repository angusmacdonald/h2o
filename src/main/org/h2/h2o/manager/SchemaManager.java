package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManager implements SchemaManagerRemote { //, ISchemaManager, Migratable
	
	/**
	 * Interface to the in-memory state of the schema manager.
	 */
	private ISchemaManager inMemory;

	/**
	 * Interface to the persisted state of this schema manager. This object interacts
	 * with the database to store the state of the schema manager on disk.
	 */
	private ISchemaManager persisted;

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
	 * Whether the schema manager has been shutdown.
	 */
	private boolean shutdown = false;
	
	/**
	 * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
	 */
	private long migrationTime = 0l;
	
	private IChordRemoteReference location;

	/**
	 * The timeout period for migrating the schema manager.
	 */
	private static final int MIGRATION_TIMEOUT = 10000;

	private LookupPinger pingerThread;
	
	public SchemaManager(Database db, boolean createTables) {

		try {
			this.inMemory = new InMemorySchemaManager(db);
			this.persisted = new PersistentSchemaManager(db, createTables);
			
			this.location = db.getChordInterface().getLocalChordReference();
			this.pingerThread = new LookupPinger(db.getRemoteInterface(), db.getChordInterface(), location);
			this.pingerThread.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/******************************************************************
	 ****	Methods which require both in memory and persisted data structures to be updated.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addConnectionInformation(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceRemote remoteDatabase)
	throws RemoteException, MovedException {
		preMethodTest();

		inMemory.addConnectionInformation(databaseURL, remoteDatabase);
		return persisted.addConnectionInformation(databaseURL, remoteDatabase);

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#addReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo ti) throws RemoteException, MovedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to add a single replica to the system: " + ti);
		preMethodTest();
		inMemory.addReplicaInformation(ti);
		persisted.addReplicaInformation(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.DataManagerRemote, org.h2.h2o.TableInfo)
	 */
	@Override
	public boolean addTableInformation(DataManagerRemote dataManager, TableInfo tableDetails) throws RemoteException, MovedException {
		preMethodTest();
		boolean result = inMemory.addTableInformation(dataManager, tableDetails);
		persisted.addTableInformation(dataManager, tableDetails);

		return result;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.removeReplicaInformation(ti);
		persisted.removeReplicaInformation(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#removeTableInformation(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean removeTableInformation(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		boolean result = inMemory.removeTableInformation(ti);
		persisted.removeTableInformation(ti);

		return result;
	}

	/******************************************************************
	 ****	Methods which only require checking in memory data structures.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.exists(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getAllTablesInSchema(java.lang.String)
	 */
	@Override
	public Set<String> getAllTablesInSchema(String schemaName)
	throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getAllTablesInSchema(schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNewTableSetNumber()
	 */
	@Override
	public int getNewTableSetNumber() throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getNewTableSetNumber();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#getNumberofReplicas(java.lang.String, java.lang.String)
	 */
	@Override
	public int getNumberofReplicas(String tableName, String schemaName)
	throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getNumberofReplicas(tableName, schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISchemaManager#lookup(java.lang.String)
	 */
	@Override
	public DataManagerRemote lookup(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.lookup(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState(ISchemaManager otherSchemaManager)
	throws RemoteException, MovedException, SQLException {
		preMethodTest();
		inMemory.buildSchemaManagerState(otherSchemaManager);
		persisted.buildSchemaManagerState(otherSchemaManager);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#buildSchemaManagerState(org.h2.h2o.manager.ISchemaManager)
	 */
	@Override
	public void buildSchemaManagerState()
	throws RemoteException, MovedException, SQLException {
		preMethodTest();
		inMemory.buildSchemaManagerState(persisted);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceRemote> getConnectionInformation() throws RemoteException, MovedException, SQLException {
		return inMemory.getConnectionInformation();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDataManagers()
	 */
	@Override
	public Map<TableInfo, DataManagerRemote> getDataManagers()  throws RemoteException, MovedException {
		return inMemory.getDataManagers();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations()  throws RemoteException, MovedException, MovedException {
		return inMemory.getReplicaLocations();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeAllTableInformation()
	 */
	@Override
	public void removeAllTableInformation() throws RemoteException, MovedException, MovedException, MovedException  {
		preMethodTest();
		try {
			inMemory.removeAllTableInformation();
			persisted.removeAllTableInformation();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#addSchemaManagerDataLocation(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void addSchemaManagerDataLocation(
			DatabaseInstanceRemote databaseReference) throws RemoteException, MovedException, MovedException, MovedException {
		preMethodTest();
		inMemory.addSchemaManagerDataLocation(databaseReference);
		persisted.addSchemaManagerDataLocation(databaseReference);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getDatabaseInstance(databaseURL);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceRemote> getDatabaseInstances() throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getDatabaseInstances();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#removeDatabaseInstance(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void removeConnectionInformation(
			DatabaseInstanceRemote localDatabaseInstance) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.removeConnectionInformation(localDatabaseInstance);
		persisted.removeConnectionInformation(localDatabaseInstance);
	}


	private void preMethodTest() throws RemoteException, MovedException{

		if (shutdown){
			throw new RemoteException(null);
		} else if (hasMoved){
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
	 * @see org.h2.h2o.manager.ISchemaManager#prepareForMigration()
	 */
	@Override
	public synchronized void prepareForMigration(String newLocation) throws RemoteException, MovedException, MigrationException {
		preMethodTest();

		movedLocation = newLocation;

		inMigration = true;

		migrationTime = System.currentTimeMillis();
	}

	
	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#completeMigration()
	 */
	@Override
	public void completeMigration() throws RemoteException,
	MovedException, MigrationException {
		if (!inMigration){ // the migration process has timed out.
			throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
		}
		
		this.hasMoved = true;
		this.inMigration = false;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#checkConnection()
	 */
	@Override
	public void checkConnection() throws RemoteException, MovedException {
		preMethodTest();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISchemaManager#changeDataManagerLocation(org.h2.h2o.comms.remote.DataManagerRemote)
	 */
	@Override
	public void changeDataManagerLocation(DataManagerRemote stub, TableInfo tableInfo)  throws RemoteException, MovedException{
		preMethodTest();
		
		inMemory.changeDataManagerLocation(stub, tableInfo);
		persisted.changeDataManagerLocation(stub, tableInfo);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#shutdown()
	 */
	@Override
	public void shutdown(boolean shutdown) throws RemoteException, MovedException {
		this.shutdown = shutdown;
		
		if (shutdown){
			pingerThread.setRunning(false);
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#getChordReference()
	 */
	@Override
	public IChordRemoteReference getChordReference() throws RemoteException {
		return location;
	}

}
