package org.h2.test.h2o;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.h2o.remote.ChordDatabaseRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Tests the basic functionality of the schema manager.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerTests {

	private static final String BASEDIR = "db_data/unittests/";

	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_NON_SM_TEST = true;
		try {
			DeleteDbFiles.execute(BASEDIR, "schema_test", true);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		H2oProperties properties = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("schemaManagerLocation", "jdbc:h2:sm:mem:one");

		properties.saveAndClose();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
//		PersistentSchemaManager.USERNAME = "sa";
//		PersistentSchemaManager.PASSWORD = "sa";
		H2oProperties knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordDatabaseRemote.currentPort + "");
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordDatabaseRemote.currentPort + "");
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordDatabaseRemote.currentPort + "");
		knownHosts.saveAndClose();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		TestBase.closeDatabaseCompletely();

		try {
			DeleteDbFiles.execute(BASEDIR, "schema_test", true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Test that the three schema manager tables are successfully created on startup in the H2O schema.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	@Test
	public void schemaTableCreation() throws ClassNotFoundException, InterruptedException{

		Connection conn = null;
		// start the server, allows to access the database remotely
		Server server = null;

		try {
			server = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test" });
			server.start();

			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			Statement stat = conn.createStatement();

			stat.executeQuery("SELECT * FROM H2O.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H2O.H2O_REPLICA");
			stat.executeQuery("SELECT * FROM H2O.H2O_CONNECTION");


		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// stop the server
			server.stop();


		}


	}

	/**
	 * Test that the state of the schema manager classes are successfully maintained between instances.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	//@Test
	public void schemaTableCreationPersistence() throws ClassNotFoundException, InterruptedException{

		Connection conn = null;
		// start the server, allows to access the database remotely
		Server server = null;
		try {
			server = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test" });
			server.start();

			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			Statement sa = conn.createStatement();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
			sa.execute("INSERT INTO TEST VALUES(2, 'World');");

			server.shutdown();
			server.stop();

			server = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test" });

			server.start();

			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			sa = conn.createStatement();

			try{
				sa.execute("SELECT * FROM TEST;");
			} catch (SQLException e){
				fail("The TEST table was not found.");
			}

			try{
				sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			} catch (SQLException e){
				fail("The TEST table was not found.");
			}			

			ResultSet rs = sa.getResultSet();
			if (!rs.next()){
				fail("There shouldn't be a single table in the schema manager.");
			}


			if (!rs.getString(3).equals("TEST")){
				fail("This entry should be for the TEST table.");
			}
			if (!rs.getString(2).equals("PUBLIC")){
				fail("This entry should be for the PUBLIC schema.");
			}
			rs.close();

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// stop the server
			server.stop();

		}


	}

//	/**
//	 * Tests that a database is able to connect to a remote database and establish linked table connections
//	 * to all schema manager tables.
//	 * @throws SQLException
//	 * @throws InterruptedException
//	 */
//	@Test
//	public void linkedSchemaTableTest(){
//		org.h2.Driver.load();
//
//		try{
//			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
//			Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
//			Statement sa = ca.createStatement();
//			Statement sb = cb.createStatement();
//
//			sb.execute("SELECT * FROM H2O.H2O_TABLE;");
//			sb.execute("SELECT * FROM H2O.H2O_REPLICA;");
//			sb.execute("SELECT * FROM H2O.H2O_CONNECTION;");
//
//			ResultSet rs = sb.getResultSet();
//
//			if (!rs.next()){
//				fail("There should be at least one row for local instance itself.");
//			}
//
//			rs.close();
//
//			sa.execute("DROP ALL OBJECTS");
//			sb.execute("DROP ALL OBJECTS");
//			ca.close();
//			cb.close();
//
//		} catch (SQLException e){
//			fail("An Unexpected SQLException was thrown.");
//			e.printStackTrace();
//		}
//	}

	/**
	 * Tests that when a new table is added to the database it is also added to the schema manager.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testTableInsertion(){
		org.h2.Driver.load();

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sa = ca.createStatement();

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			ResultSet rs = sa.getResultSet();		
			if (rs.next()){
				fail("There shouldn't be any tables in the schema manager yet.");
			}
			rs.close();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			
			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			rs = sa.getResultSet();		
			if (rs.next()){
				assertEquals("TEST", rs.getString(3));
				assertEquals("PUBLIC", rs.getString(2));
			} else {
				fail("Table PUBLIC.TEST was not found in the schema manager.");
			}
			rs.close();

			sa.close();
			ca.close();
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that when a new table is added to the database it is accessible remotely.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testTableAccessibilityOnCreate(){
		org.h2.Driver.load();

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			Statement sa = ca.createStatement();
			Statement sb = cb.createStatement();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
			sa.execute("INSERT INTO TEST VALUES(2, 'World');");

			try{
				sb.execute("SELECT * FROM TEST;");
			} catch (SQLException e){
				e.printStackTrace();
				fail("The TEST table was not found.");
			}
			ResultSet rs = sb.getResultSet();		

			if (rs.next()){
				assertEquals(1, rs.getInt(1));
				assertEquals("Hello", rs.getString(2));
			} else {
				fail("Test was not remotely accessible.");
			}

			if (rs.next()){
				assertEquals(2, rs.getInt(1));
				assertEquals("World", rs.getString(2));
			} else {
				fail("Not all of the contents of test were remotely accessible.");
			}			

			rs.close();
			sa.close();
			sb.close();
			ca.close();
			cb.close();
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");

		}
	}

	/**
	 * Tests that when a table is dropped it isn't accessible, and not that meta-data is not available from the schema manager.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testTableAccessibilityOnDrop(){
		org.h2.Driver.load();

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sa = ca.createStatement();

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			ResultSet rs = sa.getResultSet();		
			if (rs.next()){
				fail("There shouldn't be any tables in the schema manager yet.");
			}
			rs.close();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			rs = sa.getResultSet();		
			if (rs.next()){
				assertEquals("TEST", rs.getString(3));
				assertEquals("PUBLIC", rs.getString(2));
			} else {
				fail("Table TEST was not found in the schema manager.");
			}
			rs.close();

			sa.execute("DROP TABLE TEST;");

			try {
				sa.execute("SELECT * FROM TEST");
				fail("Should have caused an exception.");
			} catch (SQLException e){
				//Expected
			}

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			rs = sa.getResultSet();		
			if (rs.next()){
				fail("There shouldn't be any entries in the schema manager.");
			} 
			rs.close();


			sa.close();
			ca.close();

		} catch (SQLException e){
			fail("An Unexpected SQLException was thrown.");
			e.printStackTrace();
		}
	}

	/**
	 * Tests that when a new table is dropped the system is able to handle this when a
	 * remote request comes in for it.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testTableRemoteAccessibilityOnDrop(){
		org.h2.Driver.load();

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			Statement sa = ca.createStatement();


			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
			sa.execute("INSERT INTO TEST VALUES(2, 'World');");


			Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sb = cb.createStatement();

			sa.execute("DROP TABLE TEST;");


			try{
				sb.execute("SELECT * FROM TEST;");
				fail("This query should fail.");
			} catch (SQLException e){
				//Expected.
			}

			sa.close();
			sb.close();
			ca.close();
			cb.close();
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");

		}
	}

	/**
	 * Tests that a primary copy is correctly set when a new table is created.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testPrimaryCopySet(){
		org.h2.Driver.load();

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sa = ca.createStatement();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT count(*) FROM H2O.H2O_REPLICA WHERE primary_copy=true;");

			ResultSet rs = sa.getResultSet();

			if (!rs.next()){
				fail("Expected one result, found none.");
			}

			if (rs.next()){
				fail("Expected one result, found more.");
			}

			rs.close();

			sa.execute("DROP TABLE TEST;");

			sa.close();
			ca.close();
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");

		}
	}

	/**
	 * Tests that where there are multiple replicas there is only one primary copy.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testPrimaryCopyUnique(){
		org.h2.Driver.load();
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");

		try{
			Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sa = ca.createStatement();

			Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
			Statement sb = cb.createStatement();

			sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sb.execute("CREATE REPLICA TEST;");

			sa.execute("SELECT * FROM H2O.H2O_REPLICA WHERE primary_copy=true;");

			ResultSet rs = sa.getResultSet();

			if (!rs.next()){
				fail("Expected one result, found none.");
			}

			if (rs.next()){
				fail("Expected one result, found more.");
			}
			rs.close();

			sa.execute("DROP TABLE TEST;");

			sa.close();
			sb.close();
			ca.close();
			cb.close();
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");

		}
	}
}
