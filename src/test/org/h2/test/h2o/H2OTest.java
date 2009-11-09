package org.h2.test.h2o;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;

/**
 * Utility class containing various methods that simulate failure in various parts of H2O. These methods are called from within the database codebase
 * rather than directly from JUnit tests so they only run if a given test constant such as Constants.IS_H2O_TEST is set to true.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class H2OTest {

	/**
	 * Removes an H2O DB instance from the RMI registy to simulate failure of a database. This method
	 * only operates when Constants.IS_H2O_TEST = true. The database that is shutdown is named "mem:two".
	 */
	public static void rmiFailure(){
		if (Constants.IS_TESTING_PRE_PREPARE_FAILURE || Constants.IS_TESTING_PRE_COMMIT_FAILURE){
			Database db = Engine.getDatabase("mem:two");
			db.removeLocalDatabaseInstance();
			
			Constants.IS_TESTING_PRE_COMMIT_FAILURE = false;
			Constants.IS_TESTING_PRE_PREPARE_FAILURE = false;
		}
	}

	/**
	 * @throws SQLException 
	 * 
	 */
	public static void queryFailure() throws SQLException {
		if (Constants.IS_TESTING_QUERY_FAILURE){
			Constants.IS_TESTING_QUERY_FAILURE = false;
			throw new SQLException("Query was deliberately sabotaged by the test harness.");
		}
	}
}
