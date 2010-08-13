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


import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.LocatorServer;
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
			DeleteDbFiles.execute(BASEDIR, "schema_test", false);
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

			server = Server.createTcpServer(new String[] { "-tcpPort", "8585", "-SMLocation", "jdbc:h2:sm:tcp://localhost:8585/db_data/unittests/schema_test" });
			server.start();

			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:8585/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

			sa = conn.createStatement();
			sa.executeUpdate("DROP ALL OBJECTS;");
			sa.executeUpdate("CREATE TABLE TEST6(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.executeUpdate("INSERT INTO TEST6 VALUES(1, 'Hello');");
			sa.executeUpdate("INSERT INTO TEST6 VALUES(2, 'World');");



			TestBase.resetLocatorFile();
			shutdownServer();

			server = Server.createTcpServer(new String[] { "-tcpPort", "8585", "-SMLocation", "jdbc:h2:sm:tcp://localhost:8585/db_data/unittests/schema_test" });
			server.start();
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:8585/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
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
