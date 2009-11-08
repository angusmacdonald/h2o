package org.h2.h2o.comms.remote;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.h2o.comms.QueryProxy;


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
	
}