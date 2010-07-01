package org.h2.h2o.manager;

import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.sql.SQLException;

import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISystemTableReference {

	/**
	 * Get a reference to the System Table. If the current System Table location is
	 * not known this method will attempt to find it.
	 * 
	 * <p>The System Table may be remote.
	 * @return Reference to the system System Table.
	 */
	public SystemTableRemote getSystemTable();

	/**
	 * Called with a 'true' parameter when the system is being shut down to allow it to ignore any
	 * exceptions that may occur if the System Table is unavailable. Just makes for tidier output when
	 * running tests.
	 * @param inShutdown If the system is being shut down any
	 * remote exceptions when contacting the System Table will be ignored.
	 * @return
	 */
	public SystemTableRemote getSystemTable(boolean inShutdown);

	/**
	 * Get the location of the System Table instance.
	 * 
	 * <p>This is the stored System Table location (i.e. the system does not have to check whether the System Table still exists at
	 * this location before returning a value).
	 * @return Stored System Table location. 
	 */
	public DatabaseURL getSystemTableURL();

	/**
	 * True if the System Table process is running locally.
	 */
	public boolean isSystemTableLocal();

	/**
	 * Attempts to find the System Table by looking up its location in the RMI registry of
	 * the database instance which is responsible for the key range containing 'System Table'.
	 * @return Reference to the system System Table.
	 * @throws SQLException If System Table registry access resulted in some kind of exception.
	 */
	public SystemTableRemote findSystemTable() throws SQLException;

	/**
	 * Returns a reference to the RMI registry of the System Table.
	 * 
	 * <p>A lookup is performed to identify where the System Table is currently located,
	 * then the registry is obtained.
	 * 
	 * <p>If the registry is not found this method returns null.
	 * @return	The RMI registry of this chord node.
	 */
	public Registry getSystemTableRegistry();

	/**
	 * Change the System Table URL. This doesn't update the actual reference to the System Table,
	 * so should only be used if the database has just entered or started a chord ring, or has just
	 * found a new System Table reference.
	 * @param systemTableURL
	 */
	public void setSystemTableURL(DatabaseURL systemTableURL);

	/**
	 * Provide a reference to the actual System Table. This is typically called when a
	 * database has just been started, or when a new System Table has been created.
	 */
	public void setSystemTable(SystemTable systemTable);

	/**
	 * Change the System Table URL and its location on chord. This doesn't update the actual reference to the System Table,
	 * so should only be used if the database has just entered or started a chord ring, or has just
	 * found a new System Table reference.
	 * @param newSMLocation
	 */
	public void setSystemTableLocation(IChordRemoteReference systemTableLocation, DatabaseURL databaseURL);

	/**
	 * True if this instance has a reference to the System Table.
	 */
	public boolean isConnectedToSM();

	/**
	 * Specify whether the System Table lookup is in the keyrange of the given chord node.
	 */
	public void setInKeyRange(boolean inKeyRange);

	/**
	 * True if the System Table chord lookup resolves to the local node. 
	 */
	public boolean isInKeyRange();

	/**
	 * Create another System Table at the current location, replacing the old manager.
	 * @param persistedSchemaTablesExist	Whether replicated copies of the System Tables state exist locally.
	 * @param recreateFromPersistedState If true the new System Table will be re-instantiated from persisted state on disk. Otherwise
	 * it will be migrated from an active in-memory copy. If the old System Table has failed the new manager must be recreated from
	 * persisted state.
	 */
	public void migrateSystemTableToLocalInstance(
			boolean persistedSchemaTablesExist,
			boolean recreateFromPersistedState);

	/**
	 * If called the System Table will be moved to the local database instance.
	 */
	public void migrateSystemTableToLocalInstance();

	/**
	 * An exception has been thrown trying to access the System Table because it has been moved to a new location. This method
	 * handles this by updating the reference to that of the new System Table.
	 * @throws SQLException 
	 */
	public void handleMovedException(MovedException e) throws SQLException;

	/**
	 * Update the reference to the new chord node responsible for the System Table key lookup.
	 * @param proxy Chord node responsible for the pointer to the System Table, but not necessarily the System Table itself.
	 */
	public void setLookupLocation(IChordRemoteReference proxy);

	/**
	 * Get the location of the chord node responsible for maintaining the pointer to the actual System Table. This may be used when
	 * a database is joining the system and has to find the System Table.
	 */
	public IChordRemoteReference getLookupLocation();

	/**
	 * Find a Table Manager for the given table in the database system.
	 * 
	 * <p>This method is a wrapper for a possibly remote System Table call. If the System Table call fails
	 * this method will check if the System Table has moved and redirect the call if it has.
	 * @param fqTableName	the table whose manager is to be found (fully qualified name includes schema name).
	 * @return	Remote reference to the Table Manager in question.
	 * @throws SQLException 	Thrown if the System Table could not be found anywhere, and lookup failed twice.
	 */
	public TableManagerRemote lookup(String fqTableName) throws SQLException;

	/**
	 * Find the Table Manager for the given table in the database system.
	 * 
	 * <p>This method is a wrapper for a possibly remote System Table call. If the System Table call fails
	 * this method will check if the System Table has moved and redirect the call if it has.
	 * @param tableInfo The table name and schema name are used in the lookup.
	 * @return
	 * @throws SQLException
	 */
	public TableManagerRemote lookup(TableInfo tableInfo) throws SQLException;

	/**
	 * Check if the node given as a parameter is the node on which the System Table is held.
	 * @param otherNode Node to check against.
	 * @return	True if the System Table is held on this node.
	 */
	public boolean isThisSystemTableNode(IChordRemoteReference otherNode);

	/**
	 * Add a new TableManager proxy to the local cache.
	 * @param tableInfo The fully qualified name of the table to be added.
	 * @param tableManager	The Table Manager to be added.
	 */
	void addProxy(TableInfo tableInfo, TableManagerRemote tableManager);

	/**
	 * Add a new Table Manager reference to the System Table.
	 * @param ti	The name of the table being added.
	 * @param tm	The reference to the extant Table Manager.
	 */
	public void addNewTableManagerReference(TableInfo ti, TableManagerRemote tm);

	/**
	 * Adds a new Table Manager to the System Table. Before doing this it stores a local reference to the Table Manager
	 * to bypass RMI calls (which are extremely inefficient).
	 * @param tableManagerRemote	The table manager being added to the System Table.
	 * @param ti					Name of the table being added.
	 * @return						True if the table was successfully added.
	 * @throws RemoteException		Thrown if the System Table could not be contacted.
	 * @throws MovedException		Thrown if the System Table has moved and a new reference is needed.
	 */
	public boolean addTableInformation(TableManagerRemote tableManagerRemote, TableInfo ti) throws RemoteException, MovedException, SQLException;
}