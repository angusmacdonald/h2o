package org.h2.h2o.manager;

import java.rmi.registry.Registry;
import java.sql.SQLException;

import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ISchemaManagerReference {

	/**
	 * Get a reference to the schema manager. If the current schema manager location is
	 * not known this method will attempt to find it.
	 * 
	 * <p>The schema manager may be remote.
	 * @return Reference to the system schema manager.
	 */
	public SchemaManagerRemote getSchemaManager();

	/**
	 * Called with a 'true' parameter when the system is being shut down to allow it to ignore any
	 * exceptions that may occur if the schema manager is unavailable. Just makes for tidier output when
	 * running tests.
	 * @param inShutdown If the system is being shut down any
	 * remote exceptions when contacting the schema manager will be ignored.
	 * @return
	 */
	public SchemaManagerRemote getSchemaManager(boolean inShutdown);

	/**
	 * Get the location of the schema manager instance.
	 * 
	 * <p>This is the stored schema manager location (i.e. the system does not have to check whether the schema manager still exists at
	 * this location before returning a value).
	 * @return Stored schema manager location. 
	 */
	public DatabaseURL getSchemaManagerURL();

	/**
	 * True if the schema manager process is running locally.
	 */
	public boolean isSchemaManagerLocal();

	/**
	 * Attempts to find the schema manager by looking up its location in the RMI registry of
	 * the database instance which is responsible for the key range containing 'schema manager'.
	 * @return Reference to the system schema manager.
	 * @throws SQLException If schema manager registry access resulted in some kind of exception.
	 */
	public SchemaManagerRemote findSchemaManager() throws SQLException;

	/**
	 * Returns a reference to the RMI registry of the schema manager.
	 * 
	 * <p>A lookup is performed to identify where the schema manager is currently located,
	 * then the registry is obtained.
	 * 
	 * <p>If the registry is not found this method returns null.
	 * @return	The RMI registry of this chord node.
	 */
	public Registry getSchemaManagerRegistry();

	/**
	 * Change the schema manager URL. This doesn't update the actual reference to the schema manager,
	 * so should only be used if the database has just entered or started a chord ring, or has just
	 * found a new schema manager reference.
	 * @param schemaManagerURL
	 */
	public void setSchemaManagerURL(DatabaseURL schemaManagerURL);

	/**
	 * Provide a reference to the actual schema manager. This is typically called when a
	 * database has just been started, or when a new schema manager has been created.
	 */
	public void setSchemaManager(SchemaManager schemaManager);

	/**
	 * Change the schema manager URL and its location on chord. This doesn't update the actual reference to the schema manager,
	 * so should only be used if the database has just entered or started a chord ring, or has just
	 * found a new schema manager reference.
	 * @param newSMLocation
	 */
	public void setSchemaManagerLocation(IChordRemoteReference schemaManagerLocation, DatabaseURL databaseURL);

	/**
	 * True if this instance has a reference to the schema manager.
	 */
	public boolean isConnectedToSM();

	/**
	 * Specify whether the schema manager lookup is in the keyrange of the given chord node.
	 */
	public void setInKeyRange(boolean inKeyRange);

	/**
	 * True if the schema manager chord lookup resolves to the local node. 
	 */
	public boolean isInKeyRange();

	/**
	 * Create another schema manager at the current location, replacing the old manager.
	 * @param persistedSchemaTablesExist	Whether replicated copies of the schema managers state exist locally.
	 * @param recreateFromPersistedState If true the new schema manager will be re-instantiated from persisted state on disk. Otherwise
	 * it will be migrated from an active in-memory copy. If the old schema manager has failed the new manager must be recreated from
	 * persisted state.
	 */
	public void migrateSchemaManagerToLocalInstance(
			boolean persistedSchemaTablesExist,
			boolean recreateFromPersistedState);

	/**
	 * If called the schema manager will be moved to the local database instance.
	 */
	public void migrateSchemaManagerToLocalInstance();

	/**
	 * An exception has been thrown trying to access the schema manager because it has been moved to a new location. This method
	 * handles this by updating the reference to that of the new schema manager.
	 * @throws SQLException 
	 */
	public void handleMovedException(MovedException e) throws SQLException;

	/**
	 * Update the reference to the new chord node responsible for the schema manager key lookup.
	 * @param proxy Chord node responsible for the pointer to the schema manager, but not necessarily the schema manager itself.
	 */
	public void setLookupLocation(IChordRemoteReference proxy);

	/**
	 * Get the location of the chord node responsible for maintaining the pointer to the actual schema manager. This may be used when
	 * a database is joining the system and has to find the schema manager.
	 */
	public IChordRemoteReference getLookupLocation();

	/**
	 * Find a data manager for the given table in the database system.
	 * 
	 * <p>This method is a wrapper for a possibly remote schema manager call. If the schema manager call fails
	 * this method will check if the schema manager has moved and redirect the call if it has.
	 * @param fqTableName	the table whose manager is to be found (fully qualified name includes schema name).
	 * @return	Remote reference to the data manager in question.
	 * @throws SQLException 	Thrown if the schema manager could not be found anywhere, and lookup failed twice.
	 */
	public DataManagerRemote lookup(String fqTableName) throws SQLException;

	/**
	 * Find the data manager for the given table in the database system.
	 * 
	 * <p>This method is a wrapper for a possibly remote schema manager call. If the schema manager call fails
	 * this method will check if the schema manager has moved and redirect the call if it has.
	 * @param tableInfo The table name and schema name are used in the lookup.
	 * @return
	 * @throws SQLException
	 */
	public DataManagerRemote lookup(TableInfo tableInfo) throws SQLException;

	/**
	 * Shutdown the database's chord node, disconnecting it from the database system.
	 */
	public void shutdown();

	/**
	 * Check if the node given as a parameter is the node on which the schema manager is held.
	 * @param otherNode Node to check against.
	 * @return	True if the schema manager is held on this node.
	 */
	public boolean isThisSchemaManagerNode(IChordRemoteReference otherNode);

}