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
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.remote.ChordRemote;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.locator.server.LocatorServer;
import org.h2o.run.AllTests;
import org.h2o.test.util.StartDatabaseInstance;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.StartupException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;


/**
 * Class which conducts tests on <i>n</i> in-memory databases running at the same time.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorDatabaseTests extends TestBase {

	private static final String BASEDIR = "db_data/multiprocesstests/";

	private LocatorServer ls;
	private static String[] dbs =  {"one", "two"};//, "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen"};
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
		Diagnostic.setLevel(DiagnosticLevel.INIT);
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

		getFullDatabaseName();
		
		for (String location: fullDbName){
			LocalH2OProperties knownHosts = new LocalH2OProperties(DatabaseURL.parseURL(location));
			knownHosts.createNewFile();
			knownHosts.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
			knownHosts.setProperty("databaseName", "testDB");
			knownHosts.saveAndClose();
		}

		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		startDatabases(true);

		sleep(5000);
		createConnectionsToDatabases();
		sleep(5000);


		//		sas = new Statement[dbs.length + 1];
		//
		//		for (int i = 0; i < dts.length; i ++){
		//			sas[i] = dts[i].getConnection().createStatement();
		//		}



	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		Constants.IS_TEAR_DOWN = true; 

		killDatabases();

		try {
			sleep(1000);
		} catch (InterruptedException e1) {};

		deleteDatabaseData();

		ls.setRunning(false);

		//		for (int i = 0; i < dbs.length; i ++){
		//			try {
		//				if (!connections[i].isClosed()) connections[i].close();
		//			} catch (SQLException e) {
		//				e.printStackTrace();
		//			}
		//		}

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
	 * Starts up every database then creates a test table on one of them.
	 */
	@Test
	public void createTestTable(){
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception");
		}
	}

	/**
	 * Starts up every database, creates a test table, kills every database, restarts, 
	 * then checks that the test table can still be accessed.
	 * @throws InterruptedException
	 */
	@Test
	public void killDatabasesThenRestart() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";
		sleep(10000);
		try {
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(getSystemTableConnection(), 1);

			sleep(10000);

			/*
			 * Kill off databases.
			 */
			killDatabases();

			sleep(10000);

			startDatabases(false);

			sleep(10000);

			createConnectionsToDatabases();

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception");
		}
	}

	/**
	 * One node gets a majority while another backs out then tries again.
	 * @throws InterruptedException 
	 */
	@Test
	public void noMajorityForOneNode() throws InterruptedException{

	}

	/**
	 * Each node gets exactly half of the locks required to create a schema manager. This checks that one of the nodes
	 * eventually gets both locks.
	 */
	@Test
	public void twoLocatorsEachProcessStuckOnOneLock(){

	}


	/**
	 * Databases restart but no System Table instances are running. They shouldn't be able to start and will fail eventually.
	 */
	@Test
	public void noSystemTableRunning() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(5000);

			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(5000);

			/*
			 * Kill off databases.
			 */
			killDatabases();

			sleep(4000);

			/*
			 * Start up all the instances which aren't System Tables.
			 */
			List<String> nonSystemTableInstances = findNonSystemTableInstances();
			String singleInstance = nonSystemTableInstances.toArray(new String[0])[0];

			startDatabases(nonSystemTableInstances);


			Connection c = createConnectionToDatabase(singleInstance);

			assertFalse(assertTestTableExists(c, 2)); //the connection should not have been created.
		} catch (SQLException e) {}

	}

	/**
	 * Databases restart but no System Table instances are running. They shouldn't be able to start initially. Then a
	 * System Table instance is started and they should connect and operate normally.
	 */
	@Test
	public void noSystemTableRunningAtFirst() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(5000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(getSystemTableConnection(), 1);

			sleep(4000);
			/*
			 * Kill off databases.
			 */
			killDatabases();


			printSystemTableInstances();

			sleep(4000);

			/*
			 * Start up all the instances which aren't System Tables.
			 */
			for (String instance: findNonSystemTableInstances()){
				startDatabase(instance);
			}

			/*
			 * Sleep, then start up all System Table instances.
			 */
			sleep(10000);
			startDatabase(findSystemTableInstance());


			createConnectionsToDatabases();

			assertTrue(assertTestTableExists(2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}

	}

	


	/**
	 * Codenamed Problem B.
	 * 
	 * Connect to existing Database instance with ST state, but find no ST running.
	 * @throws InterruptedException 
	 */
	@Test
	public void systemTableMigrationOnFailure() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(5000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(4000);

			/*
			 * Kill off the System Table process. 
			 */
			for (String instance: findSystemTableInstances()){
				killDatabase(instance);
				break;
			}

			sleep(10000);

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
	 * Codenamed Problem B.
	 * 
	 * Connect to existing Database instance with ST state and TM state, but find no ST or TM running.
	 * @throws InterruptedException 
	 */
	@Test
	public void tableManagerMigrationOnFailure() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		try {
			sleep(5000);
			/*
			 * Create test table.
			 */
			executeUpdateOnFirstMachine(sql);

			assertTestTableExists(2);
			assertMetaDataExists(connections[0], 1);

			sleep(4000);

			sql = "CREATE REPLICA TEST;";
			executeUpdateOnSecondMachine(sql);

			sleep(4000);
			
			/*
			 * Kill off the System Table process. 
			 */
			for (String instance: findSystemTableInstances()){
				killDatabase(instance);
				break;
			}

			sleep(10000);

			//createConnectionsToDatabases();

			assertTrue(assertTestTableExists(connections[1], 2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}


	/**
	 * Codenamed Problem B.
	 * 
	 * Connect to existing Database instance with ST state, but find no ST running.
	 * @throws InterruptedException 
	 */
	@Test
	public void instancesRunningButNoSystemTable() throws InterruptedException{

	}

	/**
	 * When a database is first started the locator server is not running so an error is thrown. Then the
	 * locator server is started and the database must now start.
	 * 
	 * This tests an edge case where the first time the database is started (before failing) it creates files on
	 * disk, so the second time it thinks there should be system table tables on disk already.
	 */
	@Test
	public void locatorServerNotRunning(){

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

	private void executeUpdateOnSecondMachine(String sql) throws SQLException {
		Statement s = connections[1].createStatement();
		s.executeUpdate(sql);
	}

	private void sleep(int time) throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "About to sleep for " + time/1000 + " seconds.");
		Thread.sleep(time);
	}
	
	private void printSystemTableInstances() {
		List<String> sts = findSystemTableInstances();
		Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Printing list of valid System Table Instances: ");
		for (String s: sts){
			Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Instance: " + s);
		}
	}
	
	/**
	 * Get a set of all database instances which hold system table state
	 */
	private List<String> findSystemTableInstances(){
		LocalH2OProperties persistedInstanceInformation = new LocalH2OProperties(DatabaseURL.parseURL(fullDbName[0]));
		try {
			persistedInstanceInformation.loadProperties();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

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

	/**
	 * Delete all of the database files created in these tests
	 */
	private static void deleteDatabaseData() {
		try {
			for (String db: LocatorDatabaseTests.dbs){
				DeleteDbFiles.execute(BASEDIR, db, true);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Start all the databases specified in the LocatorDatabaseTests.dbs string array.
	 */
	private void getFullDatabaseName() {
		processes = new HashMap<String, Process>();

		fullDbName = new String[dbs.length];
		for (int i = 0; i < dbs.length; i ++){
			int port = 9080 + i;
			fullDbName[i] = "jdbc:h2:sm:tcp://localhost:" + port + "/db_data/multiprocesstests/" + dbs[i];
			fullDbName[i] = DatabaseURL.parseURL(fullDbName[i]).getURL();
		}
	}
	
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
			processes.put(connectionString, ProcessInvocation.runJavaProcess(StartDatabaseInstance.class, args));
		} catch (IOException e) {
			ErrorHandling.error("Failed to create new database process.");
		} catch (UnknownPlatformException e) {
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

	/**
	 * Create JDBC connections to every database in the LocatorDatabaseTests.dbs string array.
	 */
	private void createConnectionsToDatabases() {
		connections = new Connection[dbs.length];
		for (int i = 0; i < dbs.length; i ++){
			connections[i] = createConnectionToDatabase(fullDbName[i]);
		}
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
			ErrorHandling.exceptionError(e, "Trying to connect to: " + connectionString);
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
			Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Killed off the database process running " + instance);
		}
	}

}
