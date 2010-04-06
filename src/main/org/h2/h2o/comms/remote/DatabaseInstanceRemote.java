package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.manager.ISchemaManager;
import org.h2.h2o.manager.SchemaManagerRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.TableInfo;

import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;


/**
 * Interface to a database instance. For each database instance in the H2O system there will be one DatabaseInstanceRemote
 * exposed via the system's RMI registry.
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
	 * Execute a query on this machine using the supplied query proxy (which contains permission to execute
	 * a given type of query).
	 * @param queryProxy	The query proxy for the table(s) involved in the query.
	 * @param sql			The query to be executed
	 * @return
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public int executeUpdate(QueryProxy queryProxy, String sql) throws RemoteException, SQLException;

	/**
	 * Get the location of the schema manager to which this instance is connected.
	 * @return
	 */
	public DatabaseURL getSchemaManagerLocation()  throws RemoteException;
	
	/**
	 * Allows another instance to specify that the schema manager is to be moved to this instance.
	 * @throws RemoteException
	 */
	public void moveSchemaManagerToThisInstance() throws RemoteException;

	/**
	 * @return
	 */
	public DatabaseURL getLocation() throws RemoteException;

	/**
	 * @param string
	 * @throws RemoteException 
	 */
	public int executeUpdate(String sql) throws RemoteException;

	/**
	 * @param schemaManager
	 * @throws RemoteException
	 */
	void createNewSchemaManagerBackup(ISchemaManager schemaManager)
			throws RemoteException;

	/**
	 * @param schemaManagerLocation
	 * @param databaseURL
	 * @throws RemoteException
	 */
	void setSchemaManagerLocation(IChordRemoteReference schemaManagerLocation,
			DatabaseURL databaseURL) throws RemoteException;

	/**
	 * @param ti
	 * @return
	 */
	public DataManagerRemote findDataManagerReference(TableInfo ti) throws RemoteException;


	/**
	 * @param b
	 */
	public void setAlive(boolean b) throws RemoteException;
}