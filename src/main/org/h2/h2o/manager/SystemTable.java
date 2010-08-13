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
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.h2.engine.Database;
import org.h2.h2o.autonomic.decision.RequestType;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.nds.util.ErrorHandling;
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

	/**
	 * Fields related to the migration functionality of the System Table.
	 */
	private SystemTableMigrationState migrationState;

	/**
	 * The timeout period for migrating the System Table.
	 */
	private static final int MIGRATION_TIMEOUT = 10000;

	public SystemTable(Database db, boolean createTables) throws Exception {

		this.inMemory = new InMemorySystemTable(db);
		this.persisted = new PersistentSystemTable(db, createTables);

		this.migrationState = new SystemTableMigrationState(db.getChordInterface().getLocalChordReference());
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
	 * @see org.h2.h2o.ISystemTable#confirmTableCreation(java.lang.String, org.h2.h2o.comms.remote.TableManagerRemote, org.h2.h2o.TableInfo)
	 */
	@Override
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails, Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException, MovedException {
		preMethodTest();
		boolean success;
		try {
			success = inMemory.addTableInformation(tableManager, tableDetails,replicaLocations);
			if (!success) return false;
			success = persisted.addTableInformation(tableManager, tableDetails, replicaLocations);
		} catch (SQLException e) {
			e.printStackTrace();
			success = false;
		}
		return success;
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
	 * @see org.h2.h2o.ISystemTable#lookup(java.lang.String)
	 */
	@Override
	public TableManagerWrapper lookup(TableInfo ti) throws RemoteException, MovedException {
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
	public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations()  throws RemoteException, MovedException, MovedException {
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
	 * @see org.h2.h2o.manager.ISystemTable#getDatabaseInstance(org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getDatabaseInstance(databaseURL);
	}


	@Override
	public Queue<DatabaseInstanceWrapper> getAvailableMachines(
			RequestType typeOfRequest) throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.getAvailableMachines(typeOfRequest);
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

		if (migrationState.shutdown){
			throw new RemoteException(null);
		} else if (migrationState.hasMoved){
			throw new MovedException(migrationState.movedLocation);
		}
		/*
		 * If the manager is being migrated, and has been migrated for less than 10 seconds (timeout period), throw an execption. 
		 */
		if (migrationState.inMigration){
			//If it hasn't moved, but is in the process of migration an exception will be thrown.
			long currentTimeOfMigration = System.currentTimeMillis() - migrationState.migrationTime;

			if (currentTimeOfMigration < MIGRATION_TIMEOUT) {
				throw new RemoteException("System Table is in the process of being moved.");
			} else {
				migrationState.inMigration = false; //Timeout request.
				this.migrationState.migrationTime = 0l;
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#prepareForMigration()
	 */
	@Override
	public synchronized void prepareForMigration(String newLocation) throws RemoteException, MovedException, MigrationException {
		preMethodTest();

		migrationState.movedLocation = newLocation;

		migrationState.inMigration = true;

		migrationState.migrationTime = System.currentTimeMillis();
	}


	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#completeMigration()
	 */
	@Override
	public void completeMigration() throws RemoteException,
	MovedException, MigrationException {
		if (!migrationState.inMigration){ // the migration process has timed out.
			throw new MigrationException("Migration process has timed-out. Took too long to migrate (timeout: " + MIGRATION_TIMEOUT + "ms)");
		}

		this.migrationState.hasMoved = true;
		this.migrationState.inMigration = false;

		((InMemorySystemTable)inMemory).removeTableManagerCheckerThread();
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
		this.migrationState.shutdown = shutdown;

	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.Migratable#getChordReference()
	 */
	@Override
	public IChordRemoteReference getChordReference() throws RemoteException {
		return migrationState.location;
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

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#addTableManagerStateReplica(org.h2.h2o.util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void addTableManagerStateReplica(TableInfo table,
			DatabaseURL replicaLocation, DatabaseURL primaryLocation, boolean active) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
		persisted.addTableManagerStateReplica(table, replicaLocation, primaryLocation, active);
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.manager.ISystemTable#removeTableManagerStateReplica(org.h2.h2o.util.TableInfo, org.h2.h2o.util.DatabaseURL)
	 */
	@Override
	public void removeTableManagerStateReplica(TableInfo table,
			DatabaseURL replicaLocation) throws RemoteException, MovedException {
		preMethodTest();
		inMemory.removeTableManagerStateReplica(table, replicaLocation);
		persisted.removeTableManagerStateReplica(table, replicaLocation);
	}

	@Override
	public Map<TableInfo, DatabaseURL> getPrimaryLocations()
			throws RemoteException, MovedException {
		return inMemory.getPrimaryLocations();
	}

	@Override
	public TableManagerRemote recreateTableManager(TableInfo table) throws RemoteException,
			MovedException {
		preMethodTest();
		return inMemory.recreateTableManager(table);
		
	}

	@Override
	public boolean checkTableManagerAccessibility() throws RemoteException, MovedException {
		preMethodTest();
		return inMemory.checkTableManagerAccessibility();
		
	}


}
