package org.h2.test.h2o;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.h2.h2o.deployment.H2O;
import org.h2.h2o.deployment.H2OLocator;
import org.h2.h2o.manager.PersistentSystemTable;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.Processes;

public class WrapperTests {
	private Process locatorProcess = null;
	private Process databaseProcess = null;

	private String defaultLocation = "db_data";
	private String databaseName = "MyFirstDatabase";
	private String databasePort = "9999";

	@Before
	public void setUp(){
		deleteDatabase(databasePort);
	}

	@After
	public void tearDown(){
		deleteDatabase(databasePort);
	}

	@Test
	public void startSingleDatabaseInstance() throws InterruptedException{
		Diagnostic.setLevel(DiagnosticLevel.FULL);

		
		try {
			/*
			 * Start the locator server. 
			 */
			List<String> locatorArgs = new LinkedList<String>();
			locatorArgs.add("-p29998");
			locatorArgs.add("-n" + databaseName);
			locatorArgs.add("-d");
			locatorArgs.add("-f'" + defaultLocation + "'");

			try {
				locatorProcess = Processes.runJavaProcess(H2OLocator.class, locatorArgs);
			} catch (IOException e) {
				fail("Unexpected IOException.");
			}

			Thread.sleep(1000);

			
			databasePort = "9999";

			startDatabaseInSeperateProcess(databasePort);

			/*
			 * Make application connection to the database.
			 */
			String databaseLocation = extractDatabaseLocation(databasePort, databaseName, defaultLocation);

			String databaseURL = createDatabaseURL(databasePort, NetUtils.getLocalAddress(), databaseLocation);
			
			testDatabaseAccess(databaseURL);

		} finally {
			/*
			 * Kill off processes.
			 */
			locatorProcess.destroy();
			databaseProcess.destroy();
		}
	}

	/**
	 * Query the database at the specified location.
	 * @param databaseURL
	 */
	private void testDatabaseAccess(String databaseURL) {
		try {
			Connection conn = DriverManager.getConnection(databaseURL , PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
			Statement stat = conn.createStatement();
			stat.executeUpdate("CREATE TABLE TEST (ID INT);");
			stat.executeUpdate("INSERT INTO TEST VALUES(7);");
			ResultSet rs = stat.executeQuery("SELECT * FROM TEST;");

			if (rs.next()){
				assertEquals(7, rs.getInt(1));
			} else {
				fail("Couldn't query database.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected IO Exception.");
		}
	}

	/**
	 * Start a new database instance on the port specified.
	 * @param databasePort
	 * @param databaseProcess
	 */
	private void startDatabaseInSeperateProcess(String databasePort) {
		/*
		 * Start the database instance.
		 */
		try {
			List<String> databaseArgs = new LinkedList<String>();

			databaseArgs.add("-n" + databaseName);
			databaseArgs.add("-p" + databasePort);
			databaseArgs.add("-d'" + defaultLocation + File.separator + databaseName + ".h2od'");
			databaseArgs.add("-f'" + defaultLocation + "'");

			databaseProcess = Processes.runJavaProcess(H2O.class, databaseArgs);
		} catch (IOException e) {
			fail("Unexpected IOException.");
		}
	}

	private String extractDatabaseLocation(String databasePort,
			String databaseName, String defaultLocation) {
		if (defaultLocation != null){
			if (!defaultLocation.endsWith("/") && !defaultLocation.endsWith("\\")){ //add a trailing slash if it isn't already there.
				defaultLocation = defaultLocation + "/";
			}
		}
		String databaseLocation = ((defaultLocation != null)? defaultLocation + "/": "") + databaseName + databasePort;
		return databaseLocation;
	}

	private static String createDatabaseURL(String port, String hostname,
			String databaseLocation) {
		return "jdbc:h2:tcp://" + hostname + ":" + port + "/" + databaseLocation;
	}
	
	private void deleteDatabase(String databasePort) {
		try {
			DeleteDbFiles.execute(defaultLocation, databaseName + databasePort, true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
