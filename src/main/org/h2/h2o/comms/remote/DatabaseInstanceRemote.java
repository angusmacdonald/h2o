package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;


/**
 * Interface to a database instance. For each database instance in the H2O system there will be one DatabaseInstanceRemote
 * exposed via the instance's RMI registry.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DatabaseInstanceRemote extends H2ORemote, TwoPhaseCommit  {

	/**
	 * Get the JDBC URL needed to connect to this database instance.
	 * 
	 * <p>This will similar to the form: jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test
	 * @return
	 * @throws RemoteException 
	 */
	public String getConnectionString() throws RemoteException;

	/**
	 * Get the connection information for this database instance, including the instances
	 * RMI port.
	 * @return	Object containing all connection information for this database.
	 */
	public DatabaseURL getConnectionURL() throws RemoteException;

	/**
	 * Get the URL of the schema manager to which this instance is connected.
	 * @return Object containing all connection information for the schema manager.
	 */
	public DatabaseURL getSchemaManagerURL()  throws RemoteException;

	/**
	 * Execute a query on this machine using the supplied query proxy (which contains permission to execute
	 * a given type of query).
	 * @param queryProxy	The query proxy for the table(s) involved in the query.
	 * @param sql			The query to be executed
	 * @return	Result of the update.
	 * @throws RemoteException		Thrown if there were problems connecting to the instance.
	 * @throws SQLException			Thrown if there was an error in the queries execution.
	 */
	public int executeUpdate(QueryProxy queryProxy, String sql) throws RemoteException, SQLException;

	/**
	 * Execute the given SQL update on this instance. Since no query proxy is provided with this method call
	 * the database instance must request the locks needed to execute this query.
	 * @param sql	The query to be executed.
	 * @return Result of the update.
	 * @throws RemoteException		Thrown if there were problems connecting to the instance.
	 * @throws SQLException			Thrown if there was an error in the queries execution.
	 */
	public int executeUpdate(String sql) throws RemoteException, SQLException;

	/**
	 * Set the current location of the schema manager. This is typically only called by the LookupPinger thread to continually inform
	 * the node responsible for #(SM) of the schema managers location.
	 * @param schemaManagerLocation		The location in Chord of the schema manager.
	 * @param databaseURL				Object containing all connection information for the schema manager.
	 * @throws RemoteException		Thrown if there were problems connecting to the instance.
	 */
	public void setSchemaManagerLocation(IChordRemoteReference schemaManagerLocation, DatabaseURL databaseURL) throws RemoteException;

	/**
	 * Look for a reference to the specified data manager. This may be called by a Schema Manager which has just been re-instantiated from
	 * persisted state, and which doesn't have direct pointers to data manager proxies (only their location).
	 * @param tableInfo		The table name and schema name of the table to be found.
	 * @return	Remote reference to the data manager, or null if nothing was found.
	 * @throws RemoteException		Thrown if there were problems connecting to the instance.
	 */
	public DataManagerRemote findDataManagerReference(TableInfo tableInfo) throws RemoteException;


	/**
	 * Set whether this database instance is alive or being shut down.  
	 * @param alive		True if the database instance is not being shut down.
	 * @throws RemoteException		Thrown if there were problems connecting to the instance.
	 */
	public void setAlive(boolean alive) throws RemoteException;
}