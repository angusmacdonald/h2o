/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.ScriptReader;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.h2o.run.AllTests;
import org.h2o.util.LocalH2OProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * The H2 class TestScriptSimple.java repackaged as a JUnit test.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class H2SimpleTest {

    private static final String BASE_TEST_DIR = "db_data";

    private static final long SHUTDOWN_CHECK_DELAY = 2000;

    private Connection connection;

    private LocatorServer locator_server;

    protected static String baseDir = getTestDir("");

    @Before
    public void setUp() throws Exception {

        locator_server = new LocatorServer(29999, "junitLocator");
        locator_server.createNewLocatorFile();
        locator_server.start();
    }

    @After
    public void tearDown() throws Exception {

        locator_server.setRunning(false);
        while (!locator_server.isFinished()) {
            Thread.sleep(SHUTDOWN_CHECK_DELAY);
        };
    }

    @Test(timeout = 60000)
    public void largeTest() throws Exception {

        Diagnostic.trace(DiagnosticLevel.FULL);

        Constants.IS_NON_SM_TEST = true;
        final LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:db_data/test/scriptSimple"));

        properties.createNewFile();
        properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
        properties.setProperty("databaseName", "testDB");
        properties.saveAndClose();

        Constants.IS_TESTING_H2_TESTS = true;

        DeleteDbFiles.execute(baseDir, "scriptSimple", true);
        reconnect();
        final String inFile = "org/h2/test/reconnecttest.txt"; // org/h2/test/testSimple.in.txt

        final InputStream is = getClass().getClassLoader().getResourceAsStream(inFile);
        final LineNumberReader lineReader = new LineNumberReader(new InputStreamReader(is, "Cp1252"));
        final ScriptReader reader = new ScriptReader(lineReader);

        Statement query = null;
        try {

            while (true) {
                String sql = reader.readStatement();
                if (sql == null) {
                    break;
                }
                sql = sql.trim();

                Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Query: " + sql);

                if ("@reconnect".equals(sql.toLowerCase())) {
                    reconnect();
                }
                else if (sql.length() == 0) {
                    // ignore
                }
                else if (sql.toLowerCase().startsWith("select")) {
                    query = connection.createStatement();
                    final ResultSet rs = query.executeQuery(sql);
                    while (rs.next()) {
                        final String expected = reader.readStatement().trim();
                        final String got = "> " + rs.getString(1);
                        assertEquals(expected, got);
                    }
                }
                else {
                    query = connection.createStatement();
                    query.execute(sql);
                }
            }
        }
        finally {
            if (query != null) {
                query.close();
            }
            is.close();
            connection.close();
            DeleteDbFiles.execute(baseDir, "scriptSimple", true);
        }
    }

    private void reconnect() throws SQLException {

        if (connection != null) {
            connection.close();
        }

        final LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL("jdbc:h2:db_data/test/scriptSimple"));

        properties.createNewFile();
        properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
        properties.setProperty("databaseName", "testDB");
        properties.saveAndClose();

        connection = getConnection("jdbc:h2:db_data/test/scriptSimple;LOG=1;LOCK_TIMEOUT=50");
    }

    private Connection getConnection(final String url) throws SQLException {

        return DriverManager.getConnection(url, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
    }

    /**
     * Get the test directory for this test.
     * 
     * @param name
     *            the directory name suffix
     * @return the test directory
     */
    public static String getTestDir(final String name) {

        return BASE_TEST_DIR + "/test" + name;
    }
}
