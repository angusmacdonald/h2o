package org.h2.test.h2o;


import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the basic functionality of the schema manager.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManager {

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
	}

	/**
	 * Test that the three schema manager tables are successfully created on startup in the H2O schema.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void schemaTableCreation() throws ClassNotFoundException{
		try {
			// start the server, allows to access the database remotely
			Server server = Server.createTcpServer(new String[] { "-tcpPort", "9081" });
			server.start();
		
			// now use the database in your application in embedded mode
			Class.forName("org.h2.Driver");
			Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9081/data/schema_test", "sa", "sa");

			Statement stat = conn.createStatement();
			
			stat.executeQuery("SELECT * FROM H20.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H20.H2O_REPLICA");
			stat.executeQuery("SELECT * FROM H20.H2O_CONNECTION");

			conn.close();
			
			// stop the server
			server.stop();
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		}

	}
	
	/**
	 * Check that when two servers are started, the second is able to contact the remote schema manager and create linked tables.
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void linkedTableCreation() throws ClassNotFoundException{
		try {
			// start the server, allows to access the database remotely
			Server server = Server.createTcpServer(new String[] { "-tcpPort", "9081" });
			server.start();
		
			// now use the database in your application in embedded mode
			Class.forName("org.h2.Driver");
			Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9081/data/schema_test", "sa", "sa");

			Statement stat = conn.createStatement();
			
			stat.executeQuery("SELECT * FROM H20.H2O_TABLE");
			stat.executeQuery("SELECT * FROM H20.H2O_REPLICA");
			stat.executeQuery("SELECT * FROM H20.H2O_CONNECTION");

			conn.close();
			
			// stop the server
			server.stop();
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Couldn't find schema manager tables.");
		}

	}
}
