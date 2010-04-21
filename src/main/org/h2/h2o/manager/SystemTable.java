package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTable implements SystemTableRemote { //, ISystemTable, Migratable

	/**
	 * Interface to the in-memory state of the System Table.
	 */
	private ISystemTable inMemory;

	/**
	 * Interface to the persisted state of this System Table. This object interacts
	 * with the database to store the state of the System Table on disk.
	 */
	private ISystemTable persisted;

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
	 * Whether the System Table has been shutdown.
	 */
	private boolean shutdown = false;

	/**
	 * The amount of time which has elapsed since migration began. Used to timeout requests which take too long.
	 */
	private long migrationTime = 0l;

	private IChordRemoteReference location;

	/**
	 * The timeout period for migrating the System Table.
	 */
	private static final int MIGRATION_TIMEOUT = 10000;

	public SystemTable(Database db, boolean createTables) throws Exception {

			this.inMemory = new InMemorySystemTable(db);
			this.persisted = new PersistentSystemTable(db, createTables);

			this.location = db.getChordInterface().getLocalChordReference();
	}

	/******************************************************************
	 ****	Methods which require both in memory and persisted data structures to be updated.
	 ******************************************************************/

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#addConnectionInformation(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceWrapper remoteDatabase)
	throws RemoteException, MovedException {
		preMethodTest();

		try {
			inMemory.addConnectionInformation(databaseURL, remoteDatabase);

			return persisted.addConnectionInformation(databaseURL, remoteDatabase);
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#addReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void addReplicaInformation(TableInfo ti) throws RemoteException, MovedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Request to add a single replica to the system: " + ti);
		preMethodTest();
		try {
			inMemory.addReplicaInformation(ti);
			persisted.addReplicaInformation(ti);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote, org.h2.h2o.TableInfo)
	 */
	@Override
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails) throws RemoteException, MovedException {
		preMethodTest();
		boolean success;
		try {
			success = inMemory.addTableInformation(tableManager, tableDetails);
			if (!success) return false;
			persisted.addTableInformation(tableManager, tableDetails);
		} catch (SQLException e) {
			e.printStackTrace();
			success = false;
		}
		return success;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#removeReplicaInformation(org.h2.h2o.TableInfo)
	 */
	@Override
	public void removeReplicaInformation(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.removeReplicaInformation(ti);
		persisted.removeReplicaInformation(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#removeTableInformation(java.lang.String, java.lang.String)
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
	 * @see org.h2.h2o.ISystemTable#exists(java.lang.String)
	 */
	@Override
	public boolean exists(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.exists(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getAllTablesInSchema(java.lang.String)
	 */
	@Override
	public Set<String> getAllTablesInSchema(String schemaName)
	throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getAllTablesInSchema(schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getNewTableSetNumber()
	 */
	@Override
	public int getNewTableSetNumber() throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getNewTableSetNumber();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#getNumberofReplicas(java.lang.String, java.lang.String)
	 */
	@Override
	public int getNumberofReplicas(String tableName, String schemaName)
	throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getNumberofReplicas(tableName, schemaName);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.ISystemTable#lookup(java.lang.String)
	 */
	@Override
	public TableManagerRemote lookup(TableInfo ti) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.lookup(ti);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState(org.h2.h2o.manager.ISystemTable)
	 */
	@Override
	public void buildSystemTableState(ISystemTable otherSystemTable)
	throws RemoteException, MovedException, SQLException {
		preMethodTest();
		inMemory.buildSystemTableState(otherSystemTable);
		
		persisted.buildSystemTableState(otherSystemTable);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#buildSystemTableState(org.h2.h2o.manager.ISystemTable)
	 */
	@Override
	public void buildSystemTableState()
	throws RemoteException, MovedException, SQLException {
		preMethodTest();
		inMemory.buildSystemTableState(persisted);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getConnectionInformation()
	 */
	@Override
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, MovedException, SQLException {
		return inMemory.getConnectionInformation();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getTableManagers()
	 */
	@Override
	public Map<TableInfo, TableManagerWrapper> getTableManagers()  throws RemoteException, MovedException {
		return inMemory.getTableManagers();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getReplicaLocations()
	 */
	@Override
	public Map<String, Set<TableInfo>> getReplicaLocations()  throws RemoteException, MovedException, MovedException {
		return inMemory.getReplicaLocations();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#removeAllTableInformation()
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
	 * @see org.h2.h2o.manager.ISystemTable#addSystemTableDataLocation(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	@Override
	public void addStateReplicaLocation(
			DatabaseInstanceRemote databaseReference) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.addStateReplicaLocation(databaseReference);
		persisted.addStateReplicaLocation(databaseReference);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getDatabaseInstance(databaseURL);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstances()
	 */
	@Override
	public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getDatabaseInstances();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#removeDatabaseInstance(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
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
	 * @see org.h2.h2o.manager.ISystemTable#prepareForMigration()
	 */
	@Override
	public synchronized void prepareForMigration(String newLocation) throws RemoteException, MovedException, MigrationException {
		preMethodTest();

		movedLocation = newLocation;

		inMigration = true;

		migrationTime = System.currentTimeMillis();
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#completeMigration()
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
	 * @see org.h2.h2o.manager.ISystemTable#checkConnection()
	 */
	@Override
	public void checkConnection() throws RemoteException, MovedException {
		preMethodTest();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#changeTableManagerLocation(org.h2.h2o.comms.remote.TableManagerRemote)
	 */
	@Override
	public void changeTableManagerLocation(TableManagerRemote stub, TableInfo tableInfo)  throws RemoteException, MovedException{
		preMethodTest();

		inMemory.changeTableManagerLocation(stub, tableInfo);
		persisted.changeTableManagerLocation(stub, tableInfo);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#shutdown()
	 */
	@Override
	public void shutdown(boolean shutdown) throws RemoteException, MovedException {
		this.shutdown = shutdown;

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#getChordReference()
	 */
	@Override
	public IChordRemoteReference getChordReference() throws RemoteException {
		return location;
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#getLocalDatabaseInstances(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public Set<TableManagerWrapper>getLocalDatabaseInstances(DatabaseURL localMachineLocation)
			throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getLocalDatabaseInstances(localMachineLocation);
	}

}
