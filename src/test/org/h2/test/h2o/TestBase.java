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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.h2o.util.LocalH2OProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Base class for JUnit tests. Performs a basic setup of two in-memory databases which are used for the rest of testing.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TestBase {
	Connection ca = null;
	Connection cb = null;
	Statement sa = null;
	Statement sb = null;

	protected LocatorServer ls;
	private static int chordPort = 40000;

	/**
	 * The number of rows that are in the test table after the initial @see {@link #setUp()} call.
	 */
	protected static final int ROWS_IN_DATABASE = 2;

	@BeforeClass
	public static void initialSetUp(){
		Constants.IS_TEAR_DOWN = false; 
		Constants.IS_NON_SM_TEST = true;

		Diagnostic.setLevel(DiagnosticLevel.FULL);

		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();

		properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:mem:three"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();




	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		Constants.IS_TEAR_DOWN = false; 
		deleteDatabaseData("db_data/unittests/", "schema_test");
		setUpDescriptorFiles();
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		//Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		//PersistentSystemTable.USERNAME = "sa";
		//PersistentSystemTable.PASSWORD = "sa";

		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

		sa = ca.createStatement();
		sb = cb.createStatement();

		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		sa.execute(sql);
	}


	/**
	 * Delete all of the database files created in these tests
	 */
	private static void deleteDatabaseData(String baseDir, String db) {
		try{
			DeleteDbFiles.execute(baseDir, db, true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	public static void setUpDescriptorFiles() {
		//		DatabaseLocatorFile dlf = new DatabaseLocatorFile("testDB", "\\\\shell\\angus\\public_html\\databases"); 
		//		
		//		dlf.setProperties("testDB", "jdbc:h2:mem:one" + "+" + ChordRemote.currentPort);
		//		

		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:mem:one"));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();

		properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();

		properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:three"));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();


		properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + chordPort++);
		properties.saveAndClose();

	}


	/**
	 * This is done because the server doesn't release the original port when it is stopped programmatically.
	 */
	public static void resetLocatorFile() {
		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:db_data/test/scriptSimple"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.saveAndClose();

		properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.saveAndClose();

	}

	public static void setUpDescriptorFiles(String[] dbLocations, String descriptorLocation, String databaseName) {
		//		DatabaseLocatorFile dlf = new DatabaseLocatorFile("testDB", "\\\\shell\\angus\\public_html\\databases"); 
		//		
		//		dlf.setProperties("testDB", "jdbc:h2:mem:one" + "+" + ChordRemote.currentPort);
		//		

		for (String location: dbLocations){
			LocalH2OProperties knownHosts = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:mem:" + location));
			knownHosts.createNewFile();
			knownHosts.setProperty("descriptor", descriptorLocation);
			knownHosts.setProperty("databaseName", "testDB");
			knownHosts.saveAndClose();
		}

	}


	/**
	 * @throws SQLException 
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws SQLException {
		try{
			//			sa.execute("DROP TABLE IF EXISTS TEST");
			//			sb.execute("DROP TABLE IF EXISTS TEST");
			sa.execute("DROP ALL OBJECTS");
			sb.execute("DROP ALL OBJECTS");

			if (!sa.isClosed()) sa.close();
			if (!sb.isClosed())sb.close();

			if (!ca.isClosed()){
				ca.close();	
			}
			if (!cb.isClosed())cb.close();	

			closeDatabaseCompletely();

		} catch (Exception e){
			e.printStackTrace();
		}
		ca = null;
		cb = null;
		sa = null;
		sb = null;

		ls.setRunning(false);
		while (!ls.isFinished()){};
	}

	/**
	 * Close the database explicitly, in case it didn't shut down correctly between tests.
	 */
	public static void closeDatabaseCompletely() {
		obliterateRMIRegistyContents();
		Collection<Database> dbs = Engine.getInstance().closeAllDatabases();

		for (Database db: dbs){
			db.close(false);
			db.shutdownImmediately();
		}

		dbs = null;

	}

	/**
	 * Removes every object from the RMI registry.
	 */
	private static void obliterateRMIRegistyContents(){
		Registry registry = null;

		try {
			registry = LocateRegistry.getRegistry(20000);

		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {
			String[] listOfObjects = registry.list();

			for (String l: listOfObjects){
				try {
					if (!l.equals("IChordNode")){
						registry.unbind(l);
					}
				} catch (NotBoundException e) {
					fail("Failed to remove " + l + " from RMI registry.");
				}
			}

			if (registry.list().length > 0){
				fail("Somehow failed to empty RMI registry.");
			}
		} catch (Exception e) {
			//It happens for tests where the registry was not set up.
		}
	}

	/**
	 * Utility method which checks that the results of a test query match up to the set of expected values. The 'TEST'
	 * class is being used in these tests so the primary keys (int) and names (varchar/string) are required to check the
	 * validity of the resultset.
	 * @param key			The set of expected primary keys.
	 * @param secondCol		The set of expected names.
	 * @param rs			The results actually returned.
	 * @throws SQLException 
	 */
	public void validateResults(int[] pKey, String[] secondCol, ResultSet rs) throws SQLException {
		if (rs == null)
			fail("Resultset was null. Probably an incorrectly set test.");

		for (int i=0; i < pKey.length; i++){
			if (pKey[i] != 0 && secondCol[i] != null){ //indicates the entry was deleted as part of the test.
				if (rs.next()){
					assertEquals(pKey[i], rs.getInt(1));
					assertEquals(secondCol[i], rs.getString(2));

				} else {
					fail("Expected an entry here.");
				}
			}
		}

		if (rs.next()){
			System.err.println(rs.getInt(1) + ": " + rs.getString(2));
			fail("Too many entries.");
		}

		rs.close();
	}

	/**
	 * Create a replica on the second test database.
	 * @throws SQLException
	 */
	protected void createReplicaOnB() throws SQLException {
		/*
		 * Create replica on B.
		 */
		sb.execute("CREATE REPLICA TEST;");

		if (sb.getUpdateCount() != 0){
			fail("Expected update count to be '0'");
		}
	}


	/**
	 * Validate the result of a query on the first replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 */
	protected void validateOnFirstMachine(TestQuery testQuery)
	throws SQLException {
		validateOnFirstMachine(testQuery.getTableName(), testQuery.getPrimaryKey(), testQuery.getSecondColumn());
	}

	/**
	 * Validate the result of a query on the second replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry
	 */
	protected void validateOnSecondMachine(TestQuery testQuery)
	throws SQLException {
		validateOnSecondMachine(testQuery.getTableName(), testQuery.getPrimaryKey(), testQuery.getSecondColumn());
	}

	/**
	 * Validate the result of a query on the second replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 * @param pKey			Primary key value
	 * @param secondCol		Second column value in test table.
	 * @throws SQLException
	 */
	protected void validateOnSecondMachine(String tableName, int[] pKey, String[] secondCol)
	throws SQLException {
		sb.execute("SELECT LOCAL ONLY * FROM " + tableName + " ORDER BY ID;"); 
		validateResults(pKey, secondCol, sb.getResultSet());
	}


	/**
	 * Validate the result of a query on the first replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 * @param pKey			Primary key value
	 * @param secondCol		Second column value in test table.
	 * @throws SQLException
	 */
	protected void validateOnFirstMachine(String tableName, int[] pKey, String[] secondCol)
	throws SQLException {
		sa.execute("SELECT LOCAL ONLY * FROM " + tableName + " ORDER BY ID;"); 
		validateResults(pKey, secondCol, sa.getResultSet());
	}

	/**
	 * @param stat 
	 * @param tableName 
	 * @throws SQLException
	 */
	protected void createSecondTable(Statement stat, String tableName) throws SQLException {
		String sqlQuery = "CREATE TABLE " + tableName + "(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sqlQuery += "INSERT INTO " + tableName + " VALUES(4, 'Meh');";
		sqlQuery += "INSERT INTO " + tableName + " VALUES(5, 'Heh');";

		stat.execute(sqlQuery);
	}

}
