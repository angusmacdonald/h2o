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

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.test.h2o.util.StartDatabaseInstance;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.remote.ChordRemote;
import org.h2o.locator.H2OLocatorInterface;
import org.h2o.locator.LocatorServer;
import org.h2o.util.DatabaseURL;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.StartupException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.Processes;


/**
 * Class which conducts tests on <i>n</i> in-memory databases running at the same time.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class FailureTests extends TestBase {

	private static final String BASEDIR = "db_data/multiprocesstests/";

	private LocatorServer ls;
	private static String[] dbs =  {"one", "two", "three"};//, "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen"};
	//"sixteen", "seventeen", "eighteen", "nineteen", "twenty", "twenty-one", "twenty-one", "twenty-two", "twenty-three", "twenty-four", "twenty-five", "twenty-six", "twenty-seven"};
	private String[] fullDbName = null;

	Map<String, Process> processes;
	private Connection[] connections;

	/**
	 * Whether the System Table state has been replicated yet.
	 */
	public static boolean isReplicated = false;

	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_TEST = true;
		Constants.IS_NON_SM_TEST = false;

		setReplicated(false);
		deleteDatabaseData();
		ChordRemote.currentPort=40000;
	}

	public static synchronized void setReplicated(boolean b){
		isReplicated = b;
	}

	public static synchronized boolean isReplicated(){
		return isReplicated;
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();

		Constants.IS_TEAR_DOWN = false; 

		org.h2.Driver.load();

		fullDbName = getFullDatabaseName();

		for (String location: fullDbName){
			LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL(location));
			properties.createNewFile();
			properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
			properties.setProperty("databaseName", "testDB");
			properties.saveAndClose();
		}

		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		startDatabases(true);

		sleep(2000);
		createConnectionsToDatabases();

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		Constants.IS_TEAR_DOWN = true; 

		killDatabases();

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {};

		deleteDatabaseData();

		ls.setRunning(false);

		while (!ls.isFinished()){};
	}


	/*
	 * ###########################################################
	 * ###########################################################
	 * 							TESTS
	 * ###########################################################
	 * ###########################################################
	 */



	/**
	 * Connect to existing Database instance with ST state, but find no ST running.
	 * 
	 * The sleep time between failure and a new query is so short that it is the instance
	 * performing the lookup that should notice and correct the failure. The next test has a longer sleep time that checks
	 * that the maintenance mechanism notices failure and reacts.
	 * @throws InterruptedException 
	 */
	@Test
	public void systemTableMigrationOnFailureRequestingInstance() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(findSystemTableInstance());


			sleep(2000); //if 4000, location will be fixed through handlemovedexception, if 8000 fixed through predecessorChange.

			//createConnectionsToDatabases();

			Statement stat = connections[1].createStatement();
			createSecondTable(stat, "TEST2");
			assertTrue(assertTest2TableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Connect to existing Database instance with ST state, but find no ST running.
	 * 
	 * The sleep time between failure and a new query is long enough that it is maintenance mechanism
	 *  that should notice and correct the failure. The previous test has a shorter sleep time that checks
	 * that the requesting instance notices failure and reacts.
	 * @throws InterruptedException 
	 */
	@Test
	public void systemTableMigrationOnFailureMaintenanceMechanism() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(findSystemTableInstance());


			sleep(8000); //if 4000, location will be fixed through handlemovedexception, if 8000 fixed through predecessorChange.

			//createConnectionsToDatabases();

			Statement stat = connections[1].createStatement();
			createSecondTable(stat, "TEST2");
			assertTrue(assertTest2TableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Test that System Table correctly records the locations of Table Manager meta-data.
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMetaDataCorrect() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(8000); //maintenance thread should have replicated table manager meta-data.


			assertTableManagerMetaDataExists(connections[0], 2);

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}


	/**
	 * Connect to existing Database instance with ST state and TM state, but find no ST or TM running.
	 * 
	 * The failure is detected by the migration of the system table, as it is also on the failed machine and will recognise
	 * that no Table Manager is available when it repopulates its in-memory state.
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMigrationOnFailureDetectedBySystemTableMigration() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			sql = "CREATE REPLICA TEST;";
			executeUpdateOnSecondMachine(sql);

			sleep(3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(findSystemTableInstance());

			sleep(15000);

			//createConnectionsToDatabases();

			assertTrue(assertTestTableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}



	/**
	 * Connect to existing Database instance with ST state and TM state, but find no ST or TM running.
	 * 
	 * The failure is detected by the migration of the system table, as it is also on the failed machine and will recognise
	 * that no Table Manager is available when it repopulates its in-memory state.
	 * 
	 * 
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMigrationOnFailureDetectedByQueryingInstance() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			sql = "CREATE REPLICA TEST;";
			executeUpdateOnSecondMachine(sql);

			sleep(3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(findSystemTableInstance());

			sleep(4000);

			//createConnectionsToDatabases();

			assertTrue(assertTestTableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}


	/**
	 * Database instance with TM running fails, but the system table is somewhere else. Tests that it can recover.
	 * 
	 * The query which detects the failure is run locally (the next test does it through a linked table.
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMigrationOnFailureSystemTableDoesntFail() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnSecondMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			sql = "CREATE REPLICA TEST;";
			executeUpdateOnFirstMachine(sql);

			sleep(3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(fullDbName[1]);

			sleep(4000);

			//createConnectionsToDatabases();

			assertTrue(assertTestTableExists(connections[0], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Database instance with TM running fails, but the system table is somewhere else. Tests that it can recover.
	 * 
	 * The query which detects the failure is through a linked table.
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMigrationOnFailureSystemTableDoesntFailLinkedTable() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnSecondMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			sql = "CREATE REPLICA TEST;";
			executeUpdateOnFirstMachine(sql);

			sleep(3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(fullDbName[1]);

			sleep(4000);

			//createConnectionsToDatabases();

			assertTrue(assertTestTableExists(connections[2], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Creates tables on every machine.
	 * 
	 * Kills off the machine with the SYSTEM TABLE and the table TEST.
	 * 
	 * Sleeps.
	 * 
	 * Restarts the machine that was killed off.
	 * 
	 * Sleeps.
	 * 
	 * Kills the database with TEST3.
	 * 
	 * Sleeps.
	 * 
	 * Queries every table to ensure they are still accessible.
	 */
	@Test
	public void multipleFailures() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);

			sleep("Wait for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the System Table process. 
			 */
			killDatabase(findSystemTableInstance());

			sleep("Killed off System Table database.", 15000);

			assertTrue(assertTestTableExists(connections[1], 2));

			startDatabase(0);
			createConnectionsToDatabase(0);
			//Database 0 tries to replicate to database 2, but database 2 is killed off before this can happen.
			sleep("About to kill off third database instance.", 2000); 
			killDatabase(2);
			sleep("About to test accessibility of test tables.", 1000);

			assertTrue(assertTestTableExists(connections[1], 2));
			assertTrue(assertTest2TableExists(connections[1], 2));
			assertTrue(assertTest3TableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Creates tables on every machine.
	 * 
	 * Kills off the machine with the SYSTEM TABLE and the table TEST.
	 * 
	 * Sleeps.
	 * 
	 * Restarts the machine that was killed off.
	 * 
	 * Sleeps.
	 * 
	 * Kills the database with TEST3.
	 * 
	 * Sleeps.
	 * 
	 * Queries every table to ensure they are still accessible.
	 */
	@Test
	public void multipleSTFailures() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			executeUpdateOnNthMachine(create1, 2);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			executeUpdateOnNthMachine(create2, 2);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);
			executeUpdateOnNthMachine(create3, 1);

			sleep("Waiting for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			Set<String> activeConnections = new HashSet<String>();
			for (Connection c: connections){
				activeConnections.add(c.getMetaData().getURL());
			}

			/*
			 * Kill off the System Table process. 
			 */
			String st = findSystemTableInstance();
			activeConnections.remove(st);
			killDatabase(st);

			sleep("Killed off System Table database.", 15000);

			st = findSystemTableInstance();
			activeConnections.remove(st);
			killDatabase(st);

			sleep("Killed of another System Table database.", 15000);

			Connection activeConnection = null;
			for (Connection c: connections){
				if (activeConnections.contains(c.getMetaData().getURL())){
					activeConnection = c;
					break;
				}
			}
			
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Active connection to use: " + activeConnection.getMetaData().getURL());
			
		//	assertTrue(assertTestTableExists(activeConnection, 2));
			assertTrue(assertTest2TableExists(activeConnection, 2));
			assertTrue(assertTest3TableExists(activeConnection, 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}
	
	/**
	 * Kill off two database instances holding table managers and try to access the tables. There are replicas
	 * of these tables on the remaining active machine, but the third table cannot be accessed because its table manager
	 * state wasn't sufficiently replicated.
	 * @throws InterruptedException
	 */
	@Test
	public void killOffNonSTMachines() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);

			sleep("Wait for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the non-system table instances.
			 */
			killDatabase(1);
			killDatabase(2);
			sleep("Killed off two databases.", 15000);

			assertTrue(assertTestTableExists(connections[0], 2));
			assertTrue(assertTest2TableExists(connections[0], 2));
			
			try {
				assertTrue(assertTest3TableExists(connections[0], 2));
			} catch (SQLException e1){
				//expected.
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	
	/**
	 * Same as {@link #killOffNonSTMachines()} but this time the third table should be accessible. There is a gap between killing
	 * off machines that should allow it to re-replicate its state.
	 * @throws InterruptedException
	 */
	@Test
	public void killOffNonSTMachines2() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);

			sleep("Wait for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the non-system table instances.
			 */
			killDatabase(2);
			sleep("Waiting to kill off second database.", 10000);
			killDatabase(1);
			sleep("Killed off two databases.", 15000);

			assertTrue(assertTestTableExists(connections[0], 2));
			assertTrue(assertTest2TableExists(connections[0], 2));
			assertTrue(assertTest3TableExists(connections[0], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}
	

	/**
	 * Kills of a DB instance, then queries a table that requires the table manager to be moved. Then
	 * restarts the old instance and checks that it can access the table through the new table manager.
	 */
	@Test
	public void killOffTMqueryThenRestartMachine() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);

			sleep("Wait for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the non-system table instances.
			 */
			killDatabase(2);
			sleep("Killed off database.", 15000);
			assertTrue(assertTest3TableExists(connections[0], 2));
			
			startDatabase(2);
			sleep("Restarted old database.", 10000);
			connections[2] = createConnectionToDatabase(fullDbName[2]);
			assertTrue(assertTest3TableExists(connections[2], 2));
			
			//Query through restarted database.
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	/**
	 * Kills of a DB instance, then queries a table that requires the table manager to be moved. Then
	 * restarts the old instance and checks that it can access the table through the new table manager.
	 * 
	 * Then the TM is killed off again (along with the whole database instance) and the table is queried again.
	 */
	@Test
	public void killOffTMqueryThenRestartMachineTwice() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
		String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
		String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

		try {
			sleep(1000);
			/*
			 * Create test table.
			 */
			executeUpdateOnNthMachine(create1, 0);
			executeUpdateOnNthMachine(create2, 1);

			executeUpdateOnNthMachine(create3, 2);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 3);

			sleep(2000);

			create1 = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(create1, 1);
			create2 = "CREATE REPLICA TEST2";
			executeUpdateOnNthMachine(create2, 0);
			create3 = "CREATE REPLICA TEST3";
			executeUpdateOnNthMachine(create3, 0);

			sleep("Wait for create replica commands to execute.", 3000);

			assertTrue(assertTestTableExists(connections[1], 2));

			/*
			 * Kill off the non-system table instances.
			 */
			killDatabase(2);
			sleep("Killed off database.", 15000);
			assertTrue(assertTest3TableExists(connections[0], 2));
			
			startDatabase(2);
			sleep("Restarted old database.", 10000);
			connections[2] = createConnectionToDatabase(fullDbName[2]);
			assertTrue(assertTest3TableExists(connections[2], 2));
			
			killDatabase(0);
			sleep("Killed off database.", 5000);
			assertTrue(assertTest3TableExists(connections[2], 2));
			
			startDatabase(0);
			sleep("Restarted old database.", 5000);
			connections[0] = createConnectionToDatabase(fullDbName[0]);
			assertTrue(assertTest3TableExists(connections[0], 2));
			
			
			//Query through restarted database.
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	
	/*
	 * ###########################################################
	 * ###########################################################
	 * 						UTILITY METHODS
	 * ###########################################################
	 * ###########################################################
	 */


	private void executeUpdateOnFirstMachine(String sql) throws SQLException {
		Statement s = connections[0].createStatement();
		s.executeUpdate(sql);
	}


	private void sleep(String message, int time) throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, message.toUpperCase() + " SLEEPING FOR " + time/1000 + " SECONDS.");
		Thread.sleep(time);
	}
	private void sleep(int time) throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ">>>>> SLEEPING FOR " + time/1000 + " SECONDS.");
		Thread.sleep(time);
	}

	private void executeUpdateOnSecondMachine(String sql) throws SQLException {
		Statement s = connections[1].createStatement();
		s.executeUpdate(sql);
	}

	private void executeUpdateOnNthMachine(String sql, int machineNumber) throws SQLException {
		Statement s = connections[machineNumber].createStatement();
		s.executeUpdate(sql);
	}

	/**
	 * Get a set of all database instances which hold system table state
	 */
	private List<String> findSystemTableInstances(){
		LocalH2OProperties persistedInstanceInformation = new LocalH2OProperties(DatabaseURL.parseURL(fullDbName[0]));
		persistedInstanceInformation.loadProperties();

		/*
		 * Contact descriptor for SM locations.
		 */
		String descriptorLocation = persistedInstanceInformation.getProperty("descriptor");
		String databaseName = persistedInstanceInformation.getProperty("databaseName");

		List<String> locations = null;

		try {
			H2OLocatorInterface dl = new H2OLocatorInterface(databaseName, descriptorLocation);
			locations = dl.getLocations();
		} catch (IOException e) {
			fail("Failed to find System Table locations.");
		} catch (StartupException e) {
			fail("Failed to find System Table locations.");
		}

		/*
		 * Parse these locations to ensure they are of the correct form.
		 */
		List<String> parsedLocations = new LinkedList<String>();
		for (String l: locations){
			parsedLocations.add(DatabaseURL.parseURL(l).getURL());
		}

		return parsedLocations;
	}

	private String findSystemTableInstance() {
		return findSystemTableInstances().get(0);
	}

	private Connection getSystemTableConnection() {
		for (String instance: findSystemTableInstances()){
			DatabaseURL dbURL = DatabaseURL.parseURL(instance);
			for (int i = 0; i < connections.length; i++){
				String connectionURL;
				try {
					connectionURL = connections[i].getMetaData().getURL();
					if (connectionURL.equals(dbURL.getURL())){
						return connections[i];
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return null; //none found.
	}

	/**
	 * Get a set of all database instances which don't hold System Table state.
	 */
	private List<String> findNonSystemTableInstances(){
		List<String> systemTableInstances = findSystemTableInstances();

		List<String> nonSystemTableInstances = new LinkedList<String>();

		for (String instance: this.fullDbName){

			if (!systemTableInstances.contains(instance)){
				nonSystemTableInstances.add(instance);
			} 
		}

		return nonSystemTableInstances;
	}

	/**
	 * Query the System Table's persisted state (specifically the H2O_TABLE table) and check that
	 * there are the correct number of entries.
	 * @param connection		Connection to execute the query on.
	 * @param expectedEntries	Number of entries expected in the table.
	 * @throws SQLException
	 */
	private void assertMetaDataExists(Connection connection, int expectedEntries) throws SQLException {
		String tableName = "H2O.H2O_TABLE"; //default value.
		tableName = getTableMetaTableName();

		/*
		 * Query database.
		 */
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery("SELECT * FROM " + tableName);

		int actualEntries = 0;
		while(rs.next()){
			actualEntries++;
		}

		assertEquals(expectedEntries, actualEntries);

		rs.close();
		s.close();
	}

	/**
	 * Query the System Table's persisted state (specifically the H2O.H2O_TABLEMANAGER_STATE table) and check that
	 * there are the correct number of entries.
	 * @param connection		Connection to execute the query on.
	 * @param expectedEntries	Number of entries expected in the table.
	 * @throws SQLException
	 */
	private void assertTableManagerMetaDataExists(Connection connection, int expectedEntries) throws SQLException {
		String tableName = "H2O.H2O_TABLEMANAGER_STATE";

		/*
		 * Query database.
		 */
		Statement s = connection.createStatement();
		ResultSet rs = s.executeQuery("SELECT * FROM " + tableName);

		int actualEntries = 0;
		while(rs.next()){
			actualEntries++;
		}

		assertEquals(expectedEntries, actualEntries);

		rs.close();
		s.close();
	}

	/**
	 * Get the name of the H2O meta table holding table information in the System Table. Uses reflection to access this value.
	 * @return This value will be something like 'H2O.H2O_TABLE', or null if the method couldn't 
	 * find the value using reflection.
	 */
	private String getTableMetaTableName() {
		String tableName = null;

		try {
			Field field = PersistentSystemTable.class.getDeclaredField("TABLES");
			field.setAccessible(true);
			tableName = (String) field.get(String.class);
		} catch (Exception e) {}

		return tableName;
	}

	/**
	 * Select all entries from the test table. Checks that the number of entries in the table
	 * matches the number of entries expected. Matches the contents of the first two entries as well.
	 * @param expectedEntries The number of entries that should be in the test table.
	 * @return true if the connection was active. false if the connection wasn't open.
	 * @throws SQLException
	 */
	private boolean assertTestTableExists(int expectedEntries) throws SQLException {
		return assertTestTableExists(connections[1], expectedEntries);
	}

	/**
	 * Select all entries from the test table. Checks that the number of entries in the table
	 * matches the number of entries expected. Matches the contents of the first two entries as well.
	 * @param expectedEntries The number of entries that should be in the test table.
	 * @return true if the connection was active. false if the connection wasn't open.
	 * @throws SQLException
	 */
	private boolean assertTestTableExists(Connection connnection, int expectedEntries) throws SQLException {
		Statement s = null;
		ResultSet rs = null;

		/*
		 * Query database.
		 */

		if (connnection == null || connnection.isClosed()){
			return false;
		}

		try {
			s = connnection.createStatement();
			rs = s.executeQuery("SELECT * FROM " + "TEST" + ";");

			int actualEntries = 0;
			while(rs.next()){

				if (actualEntries==0){
					assertEquals(1, rs.getInt(1));
					assertEquals("Hello", rs.getString(2));
				} else if (actualEntries==1){
					assertEquals(2, rs.getInt(1));
					assertEquals("World", rs.getString(2));
				}

				actualEntries++;
			}
			assertEquals(expectedEntries, actualEntries);
		} finally {
			if (rs != null) rs.close();
			if (s != null) s.close();
		}

		return true;
	}

	private boolean assertTest2TableExists(Connection connnection, int expectedEntries) throws SQLException {
		Statement s = null;
		ResultSet rs = null;

		/*
		 * Query database.
		 */

		if (connnection == null || connnection.isClosed()){
			return false;
		}

		try {
			s = connnection.createStatement();
			rs = s.executeQuery("SELECT * FROM " + "TEST2" + ";");

			int actualEntries = 0;
			while(rs.next()){

				if (actualEntries==0){
					assertEquals(4, rs.getInt(1));
					assertEquals("Meh", rs.getString(2));
				} else if (actualEntries==1){
					assertEquals(5, rs.getInt(1));
					assertEquals("Heh", rs.getString(2));
				}

				actualEntries++;
			}
			assertEquals(expectedEntries, actualEntries);
		} finally {
			if (rs != null) rs.close();
			if (s != null) s.close();
		}

		return true;
	}

	private boolean assertTest3TableExists(Connection connnection, int expectedEntries) throws SQLException {
		Statement s = null;
		ResultSet rs = null;

		/*
		 * Query database.
		 */

		if (connnection == null || connnection.isClosed()){
			return false;
		}

		try {
			s = connnection.createStatement();
			rs = s.executeQuery("SELECT * FROM " + "TEST3" + ";");

			int actualEntries = 0;
			while(rs.next()){

				if (actualEntries==0){
					assertEquals(4, rs.getInt(1));
					assertEquals("Clouds", rs.getString(2));
				} else if (actualEntries==1){
					assertEquals(5, rs.getInt(1));
					assertEquals("Rainbows", rs.getString(2));
				}

				actualEntries++;
			}
			assertEquals(expectedEntries, actualEntries);
		} finally {
			if (rs != null) rs.close();
			if (s != null) s.close();
		}

		return true;
	}

	/**
	 * Delete all of the database files created in these tests
	 */
	private static void deleteDatabaseData() {
		try {
			for (String db: FailureTests.dbs){
				DeleteDbFiles.execute(BASEDIR, db, true);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private String[] getFullDatabaseName() {
		processes = new HashMap<String, Process>();

		fullDbName = new String[dbs.length];
		for (int i = 0; i < dbs.length; i ++){
			int port = 9080 + i;
			fullDbName[i] = "jdbc:h2:sm:tcp://localhost:" + port + "/db_data/multiprocesstests/" + dbs[i];
			fullDbName[i] = DatabaseURL.parseURL(fullDbName[i]).getURL();
		}
		
		return fullDbName;
	}

	/**
	 * Starts all databases, ensuring the first database, 'one', will be the intial System Table if the parameter is true.
	 * @throws InterruptedException
	 */
	private void startDatabases(boolean guaranteeOneIsSystemTable) throws InterruptedException{
		for (int i = 0; i < dbs.length; i ++){
			int port = 9080 + i;
			startDatabase(fullDbName[i], port);

			if (guaranteeOneIsSystemTable && i == 0) sleep (1000);
		}
	}

	/**
	 * Start all of the databases specified.
	 * @param databasesToStart	The databases that will be started by this method.
	 */
	private void startDatabases(List<String> databasesToStart) {
		for (String instance: databasesToStart){
			startDatabase(instance);
		}
	}

	/**
	 * Start the specified database on the specified port.
	 * @param connectionString	Connection string for the database being started.
	 * @param port	Port the database will run on.
	 */
	private void startDatabase(String connectionString, int port) {
		String connectionArgument = "-l\""+ connectionString + "\"";

		List<String> args = new LinkedList<String>();
		args.add(connectionArgument);
		args.add("-p" + port);

		try {
			processes.put(connectionString, Processes.runJavaProcess(StartDatabaseInstance.class, args));
		} catch (IOException e) {
			ErrorHandling.error("Failed to create new database process.");
		}
	}

	/**
	 * Start the specified database. As the port is not specified as a parameter the connection string
	 * must be parsed to find it.
	 * @param connectionString 	Database which is about to be started.
	 */
	private void startDatabase(String connectionString) {
		// jdbc:h2:sm:tcp://localhost:9091/db_data/multiprocesstests/thirteen

		String port = connectionString.substring(connectionString.indexOf("tcp://")+"tcp://".length());
		port = port.substring(port.indexOf(":")+";".length());
		port = port.substring(0, port.indexOf("/"));

		startDatabase(connectionString, Integer.parseInt(port));
	}
	private void startDatabase(int i) {
		// jdbc:h2:sm:tcp://localhost:9091/db_data/multiprocesstests/thirteen
		String connectionString = fullDbName[i];
		String port = connectionString.substring(connectionString.indexOf("tcp://")+"tcp://".length());
		port = port.substring(port.indexOf(":")+";".length());
		port = port.substring(0, port.indexOf("/"));

		startDatabase(connectionString, Integer.parseInt(port));
	}

	/**
	 * Create JDBC connections to every database in the LocatorDatabaseTests.dbs string array.
	 */
	private void createConnectionsToDatabases() {
		connections = new Connection[dbs.length];
		for (int i = 0; i < dbs.length; i ++){
			connections[i] = createConnectionToDatabase(fullDbName[i]);
		}
	}

	private void createConnectionsToDatabase(int i) {
		connections[i] = createConnectionToDatabase(fullDbName[i]);
	}

	/**
	 * Create a connection to the database specified by the connection string parameter.
	 * @param connectionString	Database URL of the database which this method connects to.
	 * @return	The newly created connection.
	 */
	private Connection createConnectionToDatabase(String connectionString) {
		try {
			return DriverManager.getConnection(connectionString, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		} catch (SQLException e) {
			ErrorHandling.errorNoEvent("Failed to connect to: " + connectionString);
			return null;
		}
	}

	/**
	 * Kill all of the running database processes.
	 */
	private void killDatabases() {
		for (Process process: processes.values()){
			process.destroy();
		}
	}

	private void killDatabase(String instance) {
		Process p = processes.get(instance);
		if (p == null){
			fail("Test failed to work as expected.");
		} else {
			p.destroy();
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Killed off the database process running " + instance);
		}
	}

	private void killDatabase(int i) {
		killDatabase(fullDbName[i]);
	}

}
