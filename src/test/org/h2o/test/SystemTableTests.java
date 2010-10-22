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
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    public static void initialSetUp() {

        Diagnostic.setLevel(DiagnosticLevel.INIT);
        Constants.IS_NON_SM_TEST = true;
        Constants.IS_TEST = true;
        try {
            DeleteDbFiles.execute(BASEDIR, "schema_test", true);
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }

    }

    private LocatorServer ls;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {

        Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
        // PersistentSystemTable.USERNAME = "sa";
        // PersistentSystemTable.PASSWORD = "sa";

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

        try {
            DeleteDbFiles.execute(BASEDIR, "schema_test", false);
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }

        ls.setRunning(false);
        while (!ls.isFinished()) {

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
    public void schemaTableCreation() throws ClassNotFoundException, InterruptedException {

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
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("Couldn't find System Table tables.");
        }
        finally {
            try {
                conn.close();
                stat.close();
            }
            catch (final SQLException e) {

            }

            // stop the server
            server.stop();

            while (server.isRunning(false)) {
            };
        }

    }

    /**
     * Test that the state of the System Table classes are successfully maintained between instances.
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void schemaTableCreationPersistence() throws ClassNotFoundException, InterruptedException {

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

            server.shutdown();
            server.stop();

            TestBase.resetLocatorFile();

            server = Server.createTcpServer(new String[]{"-tcpPort", "9082", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test"});

            server.start();

            conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9082/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            TestBase.resetLocatorFile();

            sa = conn.createStatement();

            try {
                sa.execute("SELECT * FROM TEST;");
            }
            catch (final SQLException e) {
                fail("The TEST table was not found.");
            }

            try {
                sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            }
            catch (final SQLException e) {
                fail("The TEST table was not found.");
            }

            final ResultSet rs = sa.getResultSet();
            if (!rs.next()) {
                fail("There shouldn't be a single table in the System Table.");
            }

            if (!rs.getString(3).equals("TEST")) {
                fail("This entry should be for the TEST table.");
            }
            if (!rs.getString(2).equals("PUBLIC")) {
                fail("This entry should be for the PUBLIC schema.");
            }
            rs.close();

        }
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("Couldn't find System Table tables.");
        }
        finally {
            try {
                conn.close();
                sa.close();
            }
            catch (final SQLException e) {

            }

            // stop the server
            server.stop();

        }

    }

    // /**
    // * Tests that a database is able to connect to a remote database and establish linked table connections
    // * to all System Table tables.
    // * @throws SQLException
    // * @throws InterruptedException
    // */
    // @Test
    // public void linkedSchemaTableTest(){
    // org.h2.Driver.load();
    //
    // try{
    // Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
    // Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
    // Statement sa = ca.createStatement();
    // Statement sb = cb.createStatement();
    //
    // sb.execute("SELECT * FROM H2O.H2O_TABLE;");
    // sb.execute("SELECT * FROM H2O.H2O_REPLICA;");
    // sb.execute("SELECT * FROM H2O.H2O_CONNECTION;");
    //
    // ResultSet rs = sb.getResultSet();
    //
    // if (!rs.next()){
    // fail("There should be at least one row for local instance itself.");
    // }
    //
    // rs.close();
    //
    // sa.execute("DROP ALL OBJECTS");
    // sb.execute("DROP ALL OBJECTS");
    // ca.close();
    // cb.close();
    //
    // } catch (SQLException e){
    // fail("An Unexpected SQLException was thrown.");
    // e.printStackTrace();
    // }
    // }

    /**
     * Tests that when a new table is added to the database it is also added to the System Table.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableInsertion() {

        org.h2.Driver.load();

        try {
            final Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            final Statement sa = ca.createStatement();

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            ResultSet rs = sa.getResultSet();
            if (rs.next()) {
                fail("There shouldn't be any tables in the System Table yet.");
            }
            rs.close();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            if (rs.next()) {
                assertEquals("TEST", rs.getString(3));
                assertEquals("PUBLIC", rs.getString(2));
            }
            else {
                fail("Table PUBLIC.TEST was not found in the System Table.");
            }
            rs.close();

            sa.close();
            ca.close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");
        }
    }

    /**
     * Tests that when a new table is added to the database it is accessible remotely.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableAccessibilityOnCreate() {

        org.h2.Driver.load();

        try {
            final Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            final Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            final Statement sa = ca.createStatement();
            final Statement sb = cb.createStatement();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
            sa.execute("INSERT INTO TEST VALUES(2, 'World');");

            try {
                sb.execute("SELECT * FROM TEST;");
            }
            catch (final SQLException e) {
                e.printStackTrace();
                fail("The TEST table was not found.");
            }
            final ResultSet rs = sb.getResultSet();

            if (rs.next()) {
                assertEquals(1, rs.getInt(1));
                assertEquals("Hello", rs.getString(2));
            }
            else {
                fail("Test was not remotely accessible.");
            }

            if (rs.next()) {
                assertEquals(2, rs.getInt(1));
                assertEquals("World", rs.getString(2));
            }
            else {
                fail("Not all of the contents of test were remotely accessible.");
            }

            rs.close();
            sa.close();
            sb.close();
            ca.close();
            cb.close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");

        }
    }

    /**
     * Tests that when a table is dropped it isn't accessible, and not that meta-data is not available from the System Table.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableAccessibilityOnDrop() {

        org.h2.Driver.load();

        try {
            final Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            final Statement sa = ca.createStatement();

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            ResultSet rs = sa.getResultSet();
            if (rs.next()) {
                fail("There shouldn't be any tables in the System Table yet.");
            }
            rs.close();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

            sa.execute("SELECT * FROM H2O.H2O_TABLE;");
            rs = sa.getResultSet();
            if (rs.next()) {
                assertEquals("TEST", rs.getString(3));
                assertEquals("PUBLIC", rs.getString(2));
            }
            else {
                fail("Table TEST was not found in the System Table.");
            }
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
            if (rs.next()) {
                fail("There shouldn't be any entries in the System Table.");
            }
            rs.close();

            sa.close();
            ca.close();

        }
        catch (final SQLException e) {
            fail("An Unexpected SQLException was thrown.");
            e.printStackTrace();
        }
    }

    /**
     * Tests that when a new table is dropped the system is able to handle this when a remote request comes in for it.
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTableRemoteAccessibilityOnDrop() {

        org.h2.Driver.load();

        try {
            final Connection ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            final Statement sa = ca.createStatement();

            sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
            sa.execute("INSERT INTO TEST VALUES(2, 'World');");

            final Connection cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            final Statement sb = cb.createStatement();

            sa.execute("DROP TABLE TEST;");

            try {
                sb.execute("SELECT * FROM TEST;");
                fail("This query should fail.");
            }
            catch (final SQLException e) {
                // Expected.
            }

            sa.close();
            sb.close();
            ca.close();
            cb.close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");

        }
    }
}
