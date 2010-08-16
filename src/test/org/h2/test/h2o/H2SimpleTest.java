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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.ScriptReader;
import org.h2o.db.id.DatabaseURL;
import org.h2o.locator.LocatorServer;
import org.h2o.util.LocalH2OProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;


/**
 * The H2 class TestScriptSimple.java repackaged as a JUnit test.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class H2SimpleTest {
	private static final String BASE_TEST_DIR = "db_data";
	private Connection conn;
	private LocatorServer ls;
	protected static String baseDir = getTestDir("");

	@Before
	public void setUp() throws Exception {
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();
	}

	@After
	public void tearDown() throws Exception {
		ls.setRunning(false);
		while (!ls.isFinished()){};

	}

	@Test
	public void largeTest() throws Exception {


		Constants.IS_NON_SM_TEST = true;
		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:db_data/test/scriptSimple"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.saveAndClose();

		Constants.IS_TESTING_H2_TESTS = true;


		DeleteDbFiles.execute(baseDir, "scriptSimple", true);
		reconnect();
		String inFile = "org/h2/test/reconnecttest.txt"; //org/h2/test/testSimple.in.txt

		InputStream is = getClass().getClassLoader().getResourceAsStream(inFile);
		LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(is, "Cp1252"));
		ScriptReader reader = new ScriptReader(lineReader);
		while (true) {
			String sql = reader.readStatement();
			if (sql == null) {
				break;
			}
			sql = sql.trim();


			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Query: " + sql);

			try {

				if ("@reconnect".equals(sql.toLowerCase())) {
					reconnect();
				} else if (sql.length() == 0) {
					// ignore
				} else if (sql.toLowerCase().startsWith("select")) {
					ResultSet rs = conn.createStatement().executeQuery(sql);
					while (rs.next()) {
						String expected = reader.readStatement().trim();
						String got = "> " + rs.getString(1);
						org.junit.Assert.assertEquals(expected, got);
					}
				} else {
					conn.createStatement().execute(sql);
				}
			} catch (SQLException e) {

				System.err.println(sql);
				e.printStackTrace();
				fail("Shouldn't have thrown this exception.");
			}
		}
		is.close();
		conn.close();
		DeleteDbFiles.execute(baseDir, "scriptSimple", true);
	}

	private void reconnect() throws SQLException {
		if (conn != null) {
			conn.close();
		}

		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:db_data/test/scriptSimple"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.saveAndClose();

		conn = getConnection("jdbc:h2:db_data/test/scriptSimple;LOG=1;LOCK_TIMEOUT=50");
	}


	private Connection getConnection(String url) throws SQLException {
		org.h2.Driver.load();
		return DriverManager.getConnection(url, "sa", "");
	}

	/**
	 * Get the test directory for this test.
	 *
	 * @param name the directory name suffix
	 * @return the test directory
	 */
	public static String getTestDir(String name) {
		return BASE_TEST_DIR + "/test" + name;
	}
}
