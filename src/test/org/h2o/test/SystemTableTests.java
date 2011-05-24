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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2o.autonomic.settings.TestingSettings;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.h2o.test.fixture.TestBase;
import org.h2o.test.fixture.TestBase2;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.standrews.cs.nds.rpc.AbstractConnectionPool;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Tests the basic functionality of the System Table.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableTests {

    private static final String BASEDIR = "db_data/unittests/";

    @BeforeClass
    public static void initialSetUp() throws SQLException {

        AbstractConnectionPool.MAX_FREE_CONNECTIONS_PER_ADDRESS = 0;
        Diagnostic.setLevel(DiagnosticLevel.INIT);
        Diagnostic.addIgnoredPackage("uk.ac.standrews.cs.stachord");

        Constants.IS_NON_SM_TEST = true;
        Constants.IS_TEST = true;

        DeleteDbFiles.execute(BASEDIR, "schema_test", true);
    }

    private LocatorServer ls;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {

        TestingSettings.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";

        TestBase.setUpDescriptorFiles();

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {

        TestBase.closeDatabaseCompletely();

        DeleteDbFiles.execute(BASEDIR, "schema_test", false);

        TestBase2.shutdownRegistry();

        ls.setRunning(false);

        while (!ls.isFinished()) {
            Thread.sleep(TestBase.SHUTDOWN_CHECK_DELAY);
        }
    }

    /**
     * Test that the three System Table tables are successfully created on startup in the H2O schema.
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void schemaTableCreation() throws ClassNotFoundException, InterruptedException, SQLException {

        Connection conn = null;
        // start the server, allows to access the database remotely
        Server server = null;
        Statement stat = null;

        try {
            server = Server.createTcpServer(new String[]{"-tcpPort", "9082", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test"});
            server.start();

            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            stat = conn.createStatement();

            stat.executeQuery("SELECT * FROM H2O.H2O_TABLE");
            stat.executeQuery("SELECT * FROM H2O.H2O_CONNECTION");

        }
        finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (stat != null) {
                    stat.close();
                }
            }
            finally {
                shutdownServer(server);
            }
        }
    }

    private void shutdownServer(final Server server) throws InterruptedException {

        server.stop();

        while (server.isRunning(false)) {
            Thread.sleep(TestBase.SHUTDOWN_CHECK_DELAY);
        };
    }

    /**
     * Test that the state of the System Table classes are successfully maintained between instances.
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     * @throws IOException 
     */
    @Test
    public void schemaTableCreationPersistence() throws ClassNotFoundException, InterruptedException, SQLException, IOException {

        Constants.IS_NON_SM_TEST = false;

        Connection conn = null;
        // start the server, allows to access the database remotely
        Server server = null;
        Statement sa = null;

        try {
            TestBase.resetLocatorFile();

            server = Server.createTcpServer(new String[]{"-tcpPort", "9082", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test"});
            server.start();

            Class.forName("org.h2.Driver");
            conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            sa = conn.createStatement();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
            sa.execute("INSERT INTO TEST VALUES(2, 'World');");

            sa.close();
            conn.close();

            server.shutdown();
            server.stop();

            TestBase.resetLocatorFile();

            server = Server.createTcpServer(new String[]{"-tcpPort", "9082", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test"});

            server.start();

            conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            TestBase.resetLocatorFile();

            sa = conn.createStatement();

            sa.execute("SELECT * FROM TEST;");
            sa.execute("SELECT * FROM H2O.H2O_TABLE;");

            final ResultSet rs = sa.getResultSet();

            assertTrue("There shouldn't be a single table in the System Table.", rs.next());
            assertEquals("This entry should be for the TEST table.", rs.getString(3), "TEST");
            assertEquals("This entry should be for the PUBLIC schema.", rs.getString(2), "PUBLIC");

            rs.close();
        }
        finally {

            try {
                if (conn != null) {
                    conn.close();
                }
                if (sa != null) {
                    sa.close();
                }
            }
            finally {
                shutdownServer(server);
            }

            Constants.IS_NON_SM_TEST = true;
        }
    }

    /**
    * Tests that a database is able to connect to a remote database and establish linked table connections
    * to all System Table tables.
    * @throws SQLException
    * @throws InterruptedException
    */
    @Test
    @Ignore
    public void linkedSchemaTableTest() throws SQLException {

        final Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        final Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        final Statement sa = ca.createStatement();
        final Statement sb = cb.createStatement();

        sb.execute("SELECT * FROM H2O.H2O_TABLE;");
        sb.execute("SELECT * FROM H2O.H2O_REPLICA;");
        sb.execute("SELECT * FROM H2O.H2O_CONNECTION;");

        final ResultSet rs = sb.getResultSet();

        assertTrue("There should be at least one row for local instance itself.", rs.next());

        rs.close();

        sa.execute("DROP ALL OBJECTS");
        sb.execute("DROP ALL OBJECTS");
        sa.close();
        sb.close();
        ca.close();
        cb.close();
    }

    /**
     * Tests that when a new table is added to the database it is also added to the System Table.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableInsertion() throws SQLException {

        Connection ca = null;
        Statement sa = null;
        ResultSet rs = null;

        try {
            ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            sa = ca.createStatement();

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            assertFalse("There shouldn't be any tables in the System Table yet.", rs.next());
            rs.close();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("SELECT * FROM H2O.H2O_TABLE;");

            rs = sa.getResultSet();

            assertTrue("Table PUBLIC.TEST was not found in the System Table.", rs.next());
            assertEquals("TEST", rs.getString(3));
            assertEquals("PUBLIC", rs.getString(2));
        }
        finally {

            if (rs != null) {
                rs.close();
            }
            if (sa != null) {
                sa.close();
            }
            if (ca != null) {
                ca.close();
            }
        }
    }

    /**
     * Tests that when a new table is added to the database it is accessible remotely.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableAccessibilityOnCreate() throws SQLException {

        Connection ca = null;
        Connection cb = null;

        Statement sa = null;
        Statement sb = null;

        ResultSet rs = null;

        try {
            ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            sa = ca.createStatement();
            sb = cb.createStatement();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
            sa.execute("INSERT INTO TEST VALUES(2, 'World');");

            sb.execute("SELECT * FROM TEST;");

            rs = sb.getResultSet();

            assertTrue("Test was not remotely accessible.", rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("Hello", rs.getString(2));

            assertTrue("Not all of the contents of test were remotely accessible.", rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals("World", rs.getString(2));
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (sa != null) {
                sa.close();
            }
            if (sb != null) {
                sb.close();
            }
            if (ca != null) {
                ca.close();
            }
            if (cb != null) {
                cb.close();
            }
        }
    }

    /**
     * Tests that when a table is dropped it isn't accessible, and not that meta-data is not available from the System Table.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableAccessibilityOnDrop() throws SQLException {

        Connection ca = null;
        Statement sa = null;
        ResultSet rs = null;

        try {
            ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            sa = ca.createStatement();

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            assertFalse("There shouldn't be any tables in the System Table yet.", rs.next());
            rs.close();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            assertTrue("Table TEST was not found in the System Table.", rs.next());
            assertEquals("TEST", rs.getString(3));
            assertEquals("PUBLIC", rs.getString(2));

            rs.close();

            sa.execute("DROP TABLE TEST;");

            try {
                sa.execute("SELECT * FROM TEST");
                fail("Should have caused an exception.");
            }
            catch (final SQLException e) {
                // Expected
            }

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            assertFalse("There shouldn't be any entries in the System Table.", rs.next());
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (sa != null) {
                sa.close();
            }
            if (ca != null) {
                ca.close();
            }
        }
    }

    /**
     * Tests that when a new table is dropped the system is able to handle this when a remote request comes in for it.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableRemoteAccessibilityOnDrop() throws SQLException {

        Connection ca = null;
        Statement sa = null;
        Connection cb = null;
        Statement sb = null;

        try {
            ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            sa = ca.createStatement();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
            sa.execute("INSERT INTO TEST VALUES(2, 'World');");

            cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            sb = cb.createStatement();

            sa.execute("DROP TABLE TEST;");

            try {
                sb.execute("SELECT * FROM TEST;");
                fail("This query should fail.");
            }
            catch (final SQLException e) {
                // Expected.
            }
        }
        finally {
            if (sa != null) {
                sa.close();
            }
            if (sb != null) {
                sb.close();
            }
            if (ca != null) {
                ca.close();
            }
            if (cb != null) {
                cb.close();
            }
        }
    }
}
