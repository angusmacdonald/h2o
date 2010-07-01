package org.h2.test.h2o;


import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.h2o.manager.PersistentSystemTable;
import org.h2.h2o.util.locator.LocatorServer;
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

	private LocatorServer ls;
	
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
	//	PersistentSystemTable.USERNAME = "sa";
		//PersistentSystemTable.PASSWORD = "sa";

	}
	
	/**
	 * Starts up a TCP server, adds some stuff, then keeps it running.
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();
		TestBase.resetLocatorFile();

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
		
		ls.setRunning(false);
		while (!ls.isFinished()){};
	}

	/**
	 * Tests that a test table is still accessible when the database has been restarted. After restart this
	 * attempts to inserrt some rows into the table then issues a SELECT query.
	 * @throws ClassNotFoundException
	 */
	@Test
	public void basicRestart() throws ClassNotFoundException{
	
		try {
			
			server = Server.createTcpServer(new String[] { "-tcpPort", "9089", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9089/db_data/unittests/schema_test" });
			server.start();

			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9089/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

			sa = conn.createStatement();
			sa.executeUpdate("DROP ALL OBJECTS;");
			sa.executeUpdate("CREATE TABLE TEST6(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.executeUpdate("INSERT INTO TEST6 VALUES(1, 'Hello');");
			sa.executeUpdate("INSERT INTO TEST6 VALUES(2, 'World');");
			
			
			
			TestBase.resetLocatorFile();
			shutdownServer();

			server = Server.createTcpServer(new String[] { "-tcpPort", "9093", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9093/db_data/unittests/schema_test" });
			server.start();
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9093/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
			sa = conn.createStatement();

			try{
				sa.executeUpdate("INSERT INTO TEST6 VALUES(3, 'Hello');");
				sa.executeUpdate("INSERT INTO TEST6 VALUES(4, 'World');");
				sa.executeQuery("SELECT * FROM TEST6;");
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
				fail("There should be a  table in the System Table.");
			}



			if (!rs.getString(2).equals("PUBLIC")){
				fail("This entry should be for the PUBLIC schema.");
			}

			rs.close();

		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find System Table tables.");
		}
	}


	/**
	 * 
	 */
	private void shutdownServer() {
		server.shutdown();
		server.stop();
		while (server.isRunning(false));
	}
}
