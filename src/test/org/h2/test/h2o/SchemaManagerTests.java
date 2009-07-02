package org.h2.test.h2o;


import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.result.LocalResult;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the basic functionality of the schema manager.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerTests {

	private static final String BASEDIR = "db_data/unittests";
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		DeleteDbFiles.main(new String[] { "-dir", BASEDIR + "/schema_test", "-quiet" });
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

			new Thread(server.start()).start();
			//server.start();



			//Thread.sleep(5000);
			// now use the database in your application in embedded mode
			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test", "sa", "sa");

			Statement stat = conn.createStatement();

			stat.executeQuery("SELECT * FROM H20.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H20.H2O_REPLICA");
			stat.executeQuery("SELECT * FROM H20.H2O_CONNECTION");


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
	 * Tests that a database is able to connect to a remote database and establish linked table connections
	 * to all schema manager tables.
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void linkedSchemaTableTest() throws SQLException, InterruptedException{
		  org.h2.Driver.load();
	        Connection ca = DriverManager.getConnection("jdbc:h2:mem:one", "sa", "sa");
	        Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
	        Statement sa = ca.createStatement();
	        Statement sb = cb.createStatement();
	        sa.execute("CREATE TABLE TEST(ID INT)");
	        sa.execute("CREATE SCHEMA P");
	        sa.execute("CREATE TABLE P.TEST(X INT)");
	        sa.execute("INSERT INTO TEST VALUES(1)");
	        sa.execute("INSERT INTO P.TEST VALUES(2)");

	       
	      
//	        
//		sb.executeQuery("SELECT * FROM H20.H2O_TABLE");
//		sb.executeQuery("SELECT * FROM H20.H2O_REPLICA");
//		ResultSet rs = sb.executeQuery("SELECT * FROM H20.H2O_CONNECTION");


//		if (rs.next()){
//			System.out.println(rs.getString(1) + ", " + rs.getString(2)  + ", " + rs.getString(3) + ", " + rs.getString(4));
//		}

		sb.execute("DROP ALL OBJECTS");
		cb.close();
	}

}
