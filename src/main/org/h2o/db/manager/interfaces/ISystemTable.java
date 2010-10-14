/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.manager.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2o.autonomic.decision.IReplicaChoice;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.DatabaseInstanceRemote;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISystemTable extends Remote, IReplicaChoice {
	
	/**
	 * Find a reference for the Table Manager responsible for the given table.
	 * 
	 * @param tableName
	 *            Name of the table whose Table Manager we are looking for.
	 * @return A reference to the given table's Table Manager.
	 * @throws RemoteException
	 */
	public TableManagerWrapper lookup(TableInfo ti) throws RemoteException, MovedException;
	
	/**
	 * Checks whether a Table Manager for the given table exists in the system. If it doesn't exist then it indicates that it is possible to
	 * create a table with the given name.
	 * 
	 * @param tableName
	 *            Name of the table being checked for.
	 * @return True if the table exists in the system.
	 * @throws RemoteException
	 */
	public boolean exists(TableInfo ti) throws RemoteException, MovedException;
	
	/**
	 * Confirm that the specified table has now been created, and provide a reference to the table's data manager.
	 * 
	 * @param tableManager
	 *            Table Manager for the given table.
	 * @param replicaLocations
	 * @param session
	 * @return True if this action was successful on the System Table; otherwise false.
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public boolean addTableInformation(TableManagerRemote tableManager, TableInfo tableDetails,
			Set<DatabaseInstanceWrapper> replicaLocations) throws RemoteException, MovedException, SQLException;
	
	/**
	 * Remove Table Manager from the system. This is used when a table is being dropped completely from the database system.
	 * 
	 * <p>
	 * If the tableName parameter is NULL the entire schema will be dropped.
	 * 
	 * @param tableName
	 *            The table to be dropped.
	 * @param schemaName
	 *            the name of the schema where this table can be found.
	 * @return true if the Table Manager was dropped successfully; otherwise false.
	 * @throws RemoteException
	 */
	public boolean removeTableInformation(TableInfo ti) throws RemoteException, MovedException;
	
	/**
	 * Add information about a new database instance to the System Table.
	 * 
	 * @param databaseURL
	 *            The name and location of the new database instance.
	 * @throws SQLException
	 */
	public int addConnectionInformation(DatabaseURL databaseURL, DatabaseInstanceWrapper databaseInstanceWrapper) throws RemoteException,
			MovedException, SQLException;
	
	/**
	 * Get a new table set number from the System Table. Each number given is unique (i.e. the same number should not be given twice).
	 * 
	 * @return
	 */
	public int getNewTableSetNumber() throws RemoteException, MovedException;
	
	// /**
	// * Get the number of replicas that exist for a particular table.
	// * @param tableName The name of the table.
	// * @param schemaName The schema which this table is in. NULL is
	// acceptable, and
	// * used to indicate the default 'PUBLIC' schema.
	// * @return
	// */
	// public int getNumberofReplicas(String tableName, String schemaName)
	// throws RemoteException, MovedException;
	
	// /**
	// * Add details of a new replica at the specified location.
	// * @param ti
	// */
	// public void addReplicaInformation(TableInfo ti) throws RemoteException,
	// MovedException, SQLException;
	
	/**
	 * Returns an array of all the tables in a given database schema.
	 * 
	 * @param schemaName
	 *            the name of the schema in question.
	 * @return Array of table names from the specified schema.
	 */
	public Set<String> getAllTablesInSchema(String schemaName) throws RemoteException, MovedException;
	
	/**
	 * Build the state of this System Table object by replicating the state of another schema manager.
	 * 
	 * @param otherSystemTable
	 *            The System Table whose state is to be taken.
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public void buildSystemTableState(ISystemTable otherSystemTable) throws RemoteException, MovedException, SQLException;
	
	/**
	 * Build the state of this System Table object by replicating the state of the local persistent System Table.
	 * 
	 * @throws RemoteException
	 * @throws SQLException
	 */
	void buildSystemTableState() throws RemoteException, MovedException, SQLException;
	
	/**
	 * Returns a set of all the databases connected in the system.
	 * 
	 * @throws SQLException
	 */
	public Map<DatabaseURL, DatabaseInstanceWrapper> getConnectionInformation() throws RemoteException, MovedException, SQLException;
	
	/**
	 * Returns a map of all Table Managers in the system.
	 */
	public Map<TableInfo, TableManagerWrapper> getTableManagers() throws RemoteException, MovedException;
	
	/**
	 * Returns a map of all replicas in the database system. Key is the fully qualified name of the table, value is the set of replica
	 * locations.
	 */
	public Map<TableInfo, Set<DatabaseURL>> getReplicaLocations() throws RemoteException, MovedException;
	
	/**
	 * Remove all references to Table Managers and replicas. Used to shutdown a System Table.
	 * 
	 * @throws RemoteException
	 */
	public void removeAllTableInformation() throws RemoteException, MovedException;
	
	/**
	 * Get a remote reference to a database instance at the specified URL.
	 * 
	 * @param databaseURL
	 *            URL of the database reference.
	 * @return Remote reference to the database instance.
	 */
	public DatabaseInstanceRemote getDatabaseInstance(DatabaseURL databaseURL) throws RemoteException, MovedException;
	
	/**
	 * Get remote references to every database instance in the database system.
	 * 
	 * @return The set of all databases in the system.
	 */
	public Set<DatabaseInstanceWrapper> getDatabaseInstances() throws RemoteException, MovedException;
	
	/**
	 * Remove connection information for a database instance.
	 * 
	 * @param localDatabaseInstance
	 */
	public void removeConnectionInformation(DatabaseInstanceRemote localDatabaseInstance) throws RemoteException, MovedException;
	
	/**
	 * Get the Table Manager instances stored local to the given location.
	 * 
	 * @param localMachineLocation
	 * @return
	 */
	public Set<TableManagerWrapper> getLocalDatabaseInstances(DatabaseURL localMachineLocation) throws RemoteException, MovedException;
	
	/**
	 * Called when the location of the Table Manager is to be changed.
	 * 
	 * @param stub
	 */
	public void changeTableManagerLocation(TableManagerRemote stub, TableInfo tableInfo) throws RemoteException, MovedException;
	
	/**
	 * Add a new location where a given table managers state has been replicated.
	 * 
	 * @param table
	 *            The table whose manager has just replicated its state.
	 * @param replicaLocation
	 *            Where the state has been replicated.
	 */
	public void addTableManagerStateReplica(TableInfo table, DatabaseURL replicaLocation, DatabaseURL primaryLocation, boolean active)
			throws RemoteException, MovedException;
	
	public Map<TableInfo, DatabaseURL> getPrimaryLocations() throws RemoteException, MovedException;
	
	/**
	 * Add a location where a given table managers state was replicated.
	 * 
	 * @param table
	 *            The table whose manager has removed/lost a replica of its state.
	 * @param replicaLocation
	 *            Where the state was replicated.
	 */
	public void removeTableManagerStateReplica(TableInfo table, DatabaseURL replicaLocation) throws RemoteException, MovedException;
	
	/**
	 * Recreate the table manager for the the table specified by the parameter if it has failed.
	 * 
	 * @param table
	 * @return
	 * @throws RemoteException
	 * @throws MovedException
	 */
	public TableManagerRemote recreateTableManager(TableInfo table) throws RemoteException, MovedException;
	
	/**
	 * Instructs the System Table to check that all Table Managers are currently active.
	 * 
	 * @return
	 * @throws MovedException
	 * @throws RemoteException
	 */
	public boolean checkTableManagerAccessibility() throws RemoteException, MovedException;
	
}
