package org.h2.test.h2o;


import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RestartTests {

	private static final String BASEDIR = "db_data/unittests/";

	private Connection conn = null;
	
	private Server server = null;
	
	private Statement sa = null;
	
	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_NON_SM_TEST = true;
		try {
			DeleteDbFiles.execute(BASEDIR, "schema_test", true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
	//	PersistentSchemaManager.USERNAME = "sa";
		//PersistentSchemaManager.PASSWORD = "sa";

	}
	
	/**
	 * Starts up a TCP server, adds some stuff, then keeps it running.
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		
		server = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test" });
		server.start();

		Class.forName("org.h2.Driver");
		conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

		sa = conn.createStatement();
		
		sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
		sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
		sa.execute("INSERT INTO TEST VALUES(2, 'World');");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		// stop the server
		server.stop();

		
		TestBase.closeDatabaseCompletely();
		try {
			DeleteDbFiles.execute(BASEDIR, "schema_test", true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Tests that a test table is still accessible when the database has been restarted. After restart this
	 * attempts to inserrt some rows into the table then issues a SELECT query.
	 * @throws ClassNotFoundException
	 */
	@Test
	public void basicRestart() throws ClassNotFoundException{
	
		try {

			shutdownServer();

			startServerAndGetConnection();

			try{
				sa.execute("INSERT INTO TEST VALUES(3, 'Hello');");
				sa.execute("INSERT INTO TEST VALUES(4, 'World');");
				sa.execute("SELECT * FROM TEST;");
			} catch (SQLException e){
				e.printStackTrace();
				fail("The TEST table was not found.");
			}

			try{
				sa.execute("SELECT * FROM H2O.H2O_TABLE;");
			} catch (SQLException e){
				fail("The TEST table was not found.");
			}			

			ResultSet rs = sa.getResultSet();
			if (!rs.next()){
				fail("There should be a  table in the schema manager.");
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
		}
	}

	/**
	 * @throws SQLException
	 */
	private void startServerAndGetConnection() throws SQLException {
		server = Server.createTcpServer(new String[] { "-tcpPort", "9081", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test" });
		server.start();
		conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
		sa = conn.createStatement();
	}

	/**
	 * 
	 */
	private void shutdownServer() {
		server.shutdown();
		server.stop();
	}
}
