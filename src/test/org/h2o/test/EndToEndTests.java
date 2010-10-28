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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.junit.Test;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTests {

    // The name of the database domain.
    private static final String DATABASE_NAME = "MyFirstDatabase";

    // Where the database will be created (where persisted state is stored).
    private static final String DATABASE_LOCATION = "db_data";

    // The port on which the database's TCP JDBC server will run.
    private static final int TCP_PORT = 9999;

    // The port on which the database's web interface will run.
    private static final int WEB_PORT = 8282;

    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    private static final int INT_TEST_VALUE = 7;

    /**
     * Tests whether a new database can be created, data inserted and read back.
     * Preconditions: none
     * Postconditions: persistent database state is not present
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void simpleLifeCycle() throws SQLException, IOException {

        deleteDatabaseState();

        // Auto-commit is on by default.

        final H2O db = initDB();
        db.startDatabase();
        createAndInsertWithAutoCommit();
        assertDataIsPresent();
        db.shutdown();

        db.deleteState();
    }

    /**
     * Tests whether a database can be properly cleaned up, by running the whole life cycle twice.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void completeCleanUp() throws SQLException, IOException {

        simpleLifeCycle();
        simpleLifeCycle();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void persistence() throws SQLException, IOException {

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();
        createAndInsertWithAutoCommit();
        db.shutdown();

        db = initDB();
        db.startDatabase();
        assertDataIsPresent();
        db.shutdown();

        db.deleteState();
    }

    /**
     * Tests whether data that has been inserted but not committed is visible within the same transaction.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void updateVisibleWithinTransaction() throws SQLException, IOException {

        deleteDatabaseState();

        final H2O db = initDB();
        db.startDatabase();
        createAndInsertWithoutAutoCommit();
        assertDataIsPresent();
        db.shutdown();

        db.deleteState();
    }

    /**
     * Tests whether data that has been inserted is correctly rolled back when auto-commit is disabled and there is no explicit commit.
     * A table is created and populated in the first instantiation of the database. The second instantiation tries to read the data, which should fail.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void noAutoCommit() throws SQLException, IOException {

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();
        createAndInsertWithoutAutoCommit();
        db.shutdown();

        db = initDB();
        db.startDatabase();
        assertDataIsNotPresent();
        db.shutdown();

        db.deleteState();
    }

    // -------------------------------------------------------------------------------------------------------

    interface IDBAction {

        void execute(Connection connection) throws SQLException;
    }

    private H2O initDB() {

        return new H2O(DATABASE_NAME, TCP_PORT, DATABASE_LOCATION);
    }

    private void performAction(final IDBAction action) throws SQLException {

        Connection connection = null;
        try {
            connection = makeConnection();
            action.execute(connection);
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void createAndInsertWithAutoCommit() throws SQLException {

        doCreateAndInsert(true);
    }

    private void createAndInsertWithoutAutoCommit() throws SQLException {

        doCreateAndInsert(false);
    }

    private void doCreateAndInsert(final boolean auto_commit) throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                connection.setAutoCommit(auto_commit);
                createTable(connection);
                insertValues(connection);
            }
        });
    }

    private void assertDataIsPresent() throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                assertDataIsPresent(connection);
            }
        });
    }

    private void assertDataIsNotPresent() throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                assertDataIsNotPresent(connection);
            }
        });
    }

    private void deleteDatabaseState() throws SQLException {

        DeleteDbFiles.execute(DATABASE_LOCATION, DATABASE_NAME + TCP_PORT, true);
    }

    private void createTable(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Create a table.
            statement.executeUpdate("CREATE TABLE TEST (ID INT);");
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void insertValues(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            statement.executeUpdate("INSERT INTO TEST VALUES(" + INT_TEST_VALUE + ");");
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void assertDataIsPresent(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            // The result set should contain the single test value.
            assertTrue(result_set.next());
            assertEquals(INT_TEST_VALUE, result_set.getInt(1));
            assertFalse(result_set.next());
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void assertDataIsNotPresent(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            // The result set should be empty.
            assertFalse(result_set.next());
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void closeIfNotNull(final Statement statement) throws SQLException {

        if (statement != null) {
            statement.close();
        }
    }

    private Connection makeConnection() throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + TCP_PORT + "/" + DATABASE_LOCATION + "/" + DATABASE_NAME + TCP_PORT;

        // Create connection to the H2O database instance.
        return DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);
    }
}
