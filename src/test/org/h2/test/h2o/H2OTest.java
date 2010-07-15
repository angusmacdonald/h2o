﻿/*
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
package org.h2.test.h2o;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;

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
	 * @param allReplicas 
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
	 * @param replica.getDatabaseInstance()
	 * @throws RemoteException 
	 */
	public static void rmiFailure(DatabaseInstanceWrapper replica) throws RemoteException {
		if (Constants.IS_TESTING_PRE_PREPARE_FAILURE || Constants.IS_TESTING_PRE_COMMIT_FAILURE){

			Constants.IS_TESTING_PRE_COMMIT_FAILURE = false;
			Constants.IS_TESTING_PRE_PREPARE_FAILURE = false;

			if (replica.getDatabaseInstance().getConnectionString().contains("mem:two")){
				throw new RemoteException("Testing remote failure");
			}
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

	/**
	 * @throws SQLException 
	 * 
	 */
	public static void createTableFailure() throws SQLException {
		if (Constants.IS_TESTING_CREATETABLE_FAILURE){

			Constants.IS_TESTING_CREATETABLE_FAILURE = false;
			throw new SQLException("Query was deliberately sabotaged by the test harness.");
		}
	}


}
