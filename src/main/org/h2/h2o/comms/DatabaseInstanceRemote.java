package org.h2.h2o.comms;

import java.rmi.RemoteException;


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
	String getConnectionString() throws RemoteException;

}