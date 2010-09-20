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
package org.h2.test.h2o;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.h2.table.Table;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.id.DatabaseURL;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Class which tests the asynchronous query functionality of H2O.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class AsynchronousTests extends MultiProcessTestBase {

	/**
	 * Tests that an update can complete with only two machines.
	 * 
	 * @throws InterruptedException
	 */
	@Test(timeout = 25000)
	public void basicAsynchronousUpdate() throws InterruptedException {
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); "
				+ "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

		try {

			killDatabase(2);

			sleep(5000);

			delayQueryCommit(2);

			startDatabase(2);

			sleep("About to create recreate connections to the newly restarted database.", 2000);

			createConnectionsToDatabase(2);

			executeUpdateOnNthMachine(create1, 0);

			sleep(1000);

			/*
			 * Create test table.
			 */
			assertTestTableExists(2, 0);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			String createReplica = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(createReplica, 1);
			executeUpdateOnNthMachine(createReplica, 2);

			sleep("About to begin test.\n\n\n\n", 3000);

			String update = "INSERT INTO TEST VALUES(3, 'Third');";

			executeUpdateOnNthMachine(update, 0);

			assertTrue(assertTestTableExists(connections[0], 3));
			assertTrue(assertTestTableExists(connections[1], 3));
			assertTrue(assertTestTableExists(connections[2], 3, false));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Tests that an update will eventually commit on the third machine after the other transaction has completed.
	 * 
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void asynchronousUpdateEventuallyCommitted() throws InterruptedException {
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); "
				+ "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

		try {

			killDatabase(2);

			sleep(5000);

			delayQueryCommit(2);

			startDatabase(2);

			sleep("About to create recreate connections to the newly restarted database.", 2000);

			createConnectionsToDatabase(2);

			executeUpdateOnNthMachine(create1, 0);

			sleep(1000);

			/*
			 * Create test table.
			 */
			assertTestTableExists(2, 0);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			String createReplica = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(createReplica, 1);
			executeUpdateOnNthMachine(createReplica, 2);

			sleep("About to begin test.\n\n\n\n", 3000);

			String update = "INSERT INTO TEST VALUES(3, 'Third');";

			executeUpdateOnNthMachine(update, 0);

			assertTrue(assertTestTableExists(connections[0], 3));
			assertTrue(assertTestTableExists(connections[1], 3));

			Thread.sleep(11000);
			assertTrue(assertTestTableExists(connections[2], 3));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Tests that an update will eventually commit on the third machine after the other transaction has completed.
	 * 
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void inactiveReplicaRecognisedOnRestart() throws InterruptedException {
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); "
				+ "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

		try {

			killDatabase(2);

			sleep(5000);

			delayQueryCommit(2);

			startDatabase(2);

			sleep("About to create recreate connections to the newly restarted database.", 2000);

			createConnectionsToDatabase(2);

			executeUpdateOnNthMachine(create1, 1);

			sleep(1000);

			/*
			 * Create test table.
			 */
			assertTestTableExists(2, 1);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			String createReplica = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(createReplica, 0);
			executeUpdateOnNthMachine(createReplica, 2);

			sleep("About to begin test.\n\n\n\n", 3000);

			String update = "INSERT INTO TEST VALUES(3, 'Third');";

			executeUpdateOnNthMachine(update, 1);

			assertTrue(assertTestTableExists(connections[0], 3));
			assertTrue(assertTestTableExists(connections[1], 3));

			sleep (2000);
			
			killDatabase(1);

			sleep(5000);

//			startDatabase(1);
//
//			sleep(2000);
//
//			createConnectionsToDatabase(1);

			sleep("Wait for database to startup and reconnect.", 10000);

//			try {
//				assertFalse(assertTestTableExists(connections[2], 3));
//				fail("Expected Exception.");
//			} catch (SQLException e) {
//			}

			assertTrue(assertTestTableExists(connections[2], 3, false));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	public static void pauseThreadIfTestingAsynchronousUpdates(Table table, Settings databaseSettings, DatabaseURL dbURL, String query) {

		if (query == null || !query.contains("INSERT INTO TEST VALUES(3, 'Third')")) {
			return;
		}

		if (databaseSettings == null) {
			return;
		}

		if (!table.getName().equals("TEST")) {
			return;
		}

		boolean delay = Boolean.parseBoolean(databaseSettings.get("DELAY_QUERY_COMMIT"));

		if (delay) {
			try {
				ErrorHandling.errorNoEvent("Delay " + dbURL.getURL() + ": " + query);

				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
