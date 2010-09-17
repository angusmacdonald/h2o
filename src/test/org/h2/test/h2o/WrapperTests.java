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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.h2o.db.manager.PersistentSystemTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class WrapperTests {
	private Process locatorProcess = null;
	private Process databaseProcess = null;

	private String defaultLocation = "db_data";
	private String databaseName = "TestDB";
	private String databasePort = "7474";

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

		int locatorPort = 29990;

		try {
			/*
			 * Start the locator server. 
			 */
			List<String> locatorArgs = new LinkedList<String>();
			locatorArgs.add("-p" + locatorPort);
			locatorArgs.add("-n" + databaseName);
			locatorArgs.add("-d");
			locatorArgs.add("-f'" + defaultLocation + "'");

			try {
				locatorProcess = ProcessInvocation.runJavaProcess(H2OLocator.class, locatorArgs);
			} catch (IOException e) {
				e.printStackTrace();
				fail("Unexpected IOException.");
			} catch (UnknownPlatformException e) {
				e.printStackTrace();
				fail("Unexpected UnknownPlatformException.");
			}

			Thread.sleep(1000);

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
			if (locatorProcess != null) locatorProcess.destroy();
			if (databaseProcess != null) databaseProcess.destroy();
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

			databaseProcess = ProcessInvocation.runJavaProcess(H2O.class, databaseArgs);
		} catch (IOException e) {
			fail("Unexpected IOException.");
		} catch (UnknownPlatformException e) {
			fail("Unexpected UnknownPlatformException.");
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
