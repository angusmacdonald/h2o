package org.h2.test.h2o;

import static org.junit.Assert.assertEquals;
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
import org.h2.test.h2o.util.StartDatabaseInstance;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.remote.ChordRemote;
import org.h2o.locator.client.H2OLocatorInterface;
import org.h2o.locator.server.LocatorServer;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.StartupException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.ProcessInvocation;
import uk.ac.standrews.cs.nds.util.UnknownPlatformException;

public class MultiProcessTestBase extends TestBase {

	private static final String BASEDIR = "db_data/multiprocesstests/";
	private LocatorServer ls;
	private static String[] dbs = {"one", "two", "three"};
	protected String[] fullDbName = null;

	Map<String, Process> processes;

	protected Connection[] connections;
	/**
	 * Whether the System Table state has been replicated yet.
	 */
	public static boolean isReplicated = false;

	@BeforeClass
	public static void initialSetUp() {
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_TEST = true;
		Constants.IS_NON_SM_TEST = false;

		setReplicated(false);
		deleteDatabaseData();
		ChordRemote.currentPort=40000;
	}

	public static synchronized void setReplicated(boolean b) {
		isReplicated = b;
	}

	public static synchronized boolean isReplicated() {
		return isReplicated;
	}

	/**
	 * Delete all of the database files created in these tests
	 */
	private static void deleteDatabaseData() {
		try {
			for (String db: dbs){
				DeleteDbFiles.execute(BASEDIR, db, true);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
		//			properties.setProperty("RELATION_REPLICATION_FACTOR", "2");	
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

	protected void executeUpdateOnFirstMachine(String sql) throws SQLException {
		Statement s = connections[0].createStatement();
		s.executeUpdate(sql);
	}

	protected void sleep(String message, int time) throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, message.toUpperCase() + " SLEEPING FOR " + time/1000 + " SECONDS.");
		Thread.sleep(time);
	}

	protected void sleep(int time) throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, ">>>>> SLEEPING FOR " + time/1000 + " SECONDS.");
		Thread.sleep(time);
	}

	protected void executeUpdateOnSecondMachine(String sql) throws SQLException {
		Statement s = connections[1].createStatement();
		s.executeUpdate(sql);
	}

	protected void executeUpdateOnNthMachine(String sql, int machineNumber) throws SQLException {
		Statement s = connections[machineNumber].createStatement();
		s.executeUpdate(sql);
	}

	/**
	 * Get a set of all database instances which hold system table state
	 */
	private List<String> findSystemTableInstances() {
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

	protected void delayQueryCommit(int dbName){
		String location = fullDbName[dbName];
		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL(location));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("DELAY_QUERY_COMMIT", "true");
		properties.saveAndClose();
	}

	protected String findSystemTableInstance() {
		return findSystemTableInstances().get(0);
	}

	protected Connection getSystemTableConnection() {
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
	protected List<String> findNonSystemTableInstances() {
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
	protected void assertMetaDataExists(Connection connection, int expectedEntries) throws SQLException {
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
	protected void assertTableManagerMetaDataExists(Connection connection, int expectedEntries) throws SQLException {
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
	 * @param databaseNumber 
	 * @return true if the connection was active. false if the connection wasn't open.
	 * @throws SQLException
	 */
	protected boolean assertTestTableExists(int expectedEntries, int databaseNumber, boolean localOnly) throws SQLException {
		return assertTestTableExists(connections[databaseNumber], expectedEntries, localOnly);
	}
	protected boolean assertTestTableExists(int expectedEntries, int databaseNumber) throws SQLException {
		return assertTestTableExists(connections[databaseNumber], expectedEntries, true);
	}

	/**
	 * Select all entries from the test table. Checks that the number of entries in the table
	 * matches the number of entries expected. Matches the contents of the first two entries as well.
	 * @param expectedEntries The number of entries that should be in the test table.
	 * @param localOnly 
	 * @return true if the connection was active. false if the connection wasn't open.
	 * @throws SQLException
	 */
	protected boolean assertTestTableExists(Connection connnection, int expectedEntries, boolean localOnly) throws SQLException {
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
			if (localOnly){
				rs = s.executeQuery("SELECT LOCAL ONLY * FROM " + "TEST" + ";");
			} else {
				rs = s.executeQuery("SELECT * FROM " + "TEST" + ";");
			}

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
	protected boolean assertTestTableExists(Connection connnection, int expectedEntries) throws SQLException {
		return assertTestTableExists(connnection, expectedEntries, true);
	}

	protected boolean assertTest2TableExists(Connection connnection, int expectedEntries) throws SQLException {
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

	protected boolean assertTest3TableExists(Connection connnection, int expectedEntries) throws SQLException {
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

	public MultiProcessTestBase() {
		super();
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
	private void startDatabases(boolean guaranteeOneIsSystemTable) throws InterruptedException {
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
	protected void startDatabases(List<String> databasesToStart) {
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

	protected void startDatabase(int i) {
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

	protected void createConnectionsToDatabase(int i) {
		connections[i] = createConnectionToDatabase(fullDbName[i]);
	}

	/**
	 * Create a connection to the database specified by the connection string parameter.
	 * @param connectionString	Database URL of the database which this method connects to.
	 * @return	The newly created connection.
	 */
	protected Connection createConnectionToDatabase(String connectionString) {
		try {
			return DriverManager.getConnection(connectionString, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
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

	protected void killDatabase(String instance) {
		Process p = processes.get(instance);
		if (p == null){
			fail("Test failed to work as expected.");
		} else {
			p.destroy();
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Killed off the database process running " + instance);
		}
	}

	protected void killDatabase(int i) {
		killDatabase(fullDbName[i]);
	}

}