/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.h2o.util.LocalH2OProperties;
import org.junit.Test;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTests {

    // The name of the database domain.
    private static final String DATABASE_NAME = "end_to_end";

    // Where the database will be created (where persisted state is stored).
    private static final String DATABASE_LOCATION = "db_data";

    // The port on which the database's TCP JDBC server will run.
    private static final int TCP_PORT = 9999;
    private static final int LOCATOR_PORT = 5999;

    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    private static final String DESCRIPTOR_DIRECTORY = LocalH2OProperties.DEFAULT_CONFIG_DIRECTORY;

    H2OLocator locator;
    H2O db;

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

        startup();
        createWithAutoCommit();
        insertOneRowWithAutoCommitNoDelay();
        assertOneRowIsPresent();
        shutdown();

        db.deletePersistentState();
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

        startup();
        createWithAutoCommit();
        insertOneRowWithAutoCommitNoDelay();
        shutdown();

        startup();
        assertOneRowIsPresent();
        shutdown();

        db.deletePersistentState();
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

        startup();
        createWithoutAutoCommit();
        insertOneRowNoCommitNoDelay();
        assertOneRowIsPresent();
        shutdown();

        db.deletePersistentState();
    }

    /**
     * Tests whether data that has been inserted is correctly rolled back when auto-commit is disabled and there is no explicit commit.
     * A table is created and populated in the first instantiation of the database. The second instantiation tries to read the data, which should fail.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void rollbackWithoutAutoCommit() throws SQLException, IOException {

        deleteDatabaseState();

        startup();
        createWithoutAutoCommit();
        insertOneRowNoCommitNoDelay();
        shutdown();

        startup();
        assertDataIsNotPresent();
        shutdown();

        db.deletePersistentState();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void explicitCommit() throws SQLException, IOException {

        deleteDatabaseState();

        startup();
        createWithoutAutoCommit();
        insertOneRowExplicitCommitNoDelay();
        shutdown();

        startup();
        assertOneRowIsPresent();
        shutdown();

        db.deletePersistentState();
    }

    /**
     * Tests whether a series of values can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void multipleInserts() throws SQLException, IOException {

        final int number_of_values = 100;

        deleteDatabaseState();

        startup();
        createWithoutAutoCommit();
        insertRowsExplicitCommitNoDelay(number_of_values);
        shutdown();

        startup();
        assertDataIsPresent(number_of_values);
        shutdown();

        db.deletePersistentState();
    }

    // -------------------------------------------------------------------------------------------------------

    interface IDBAction {

        void execute(Connection connection) throws SQLException;
    }

    private void startup() throws SQLException, IOException {

        locator = new H2OLocator(DATABASE_NAME, LOCATOR_PORT, true, DESCRIPTOR_DIRECTORY);
        final String descriptorFilePath = locator.start();

        db = new H2O(DATABASE_NAME, TCP_PORT, DATABASE_LOCATION, descriptorFilePath);

        db.startDatabase();
    }

    private void shutdown() throws SQLException {

        db.shutdown();
        locator.shutdown();
    }

    private void assertOneRowIsPresent() throws SQLException {

        assertDataIsPresent(1);
    }

    private void insertOneRowWithAutoCommitNoDelay() throws SQLException {

        insertWithAutoCommit(1, 0);
    }

    private void insertOneRowNoCommitNoDelay() throws SQLException {

        insertWithoutAutoCommitWithoutExplicitCommit(1, 0);
    }

    private void insertOneRowExplicitCommitNoDelay() throws SQLException {

        insertWithoutAutoCommitWithExplicitCommit(1, 0);
    }

    private void insertRowsExplicitCommitNoDelay(final int number_of_values) throws SQLException {

        insertWithoutAutoCommitWithExplicitCommit(number_of_values, 0);
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

    private void insertWithAutoCommit(final int number_of_rows_to_insert, final long delay) throws SQLException {

        doInsert(number_of_rows_to_insert, 0, true, false, delay);
    }

    private void insertWithoutAutoCommitWithoutExplicitCommit(final int number_of_rows_to_insert, final long delay) throws SQLException {

        doInsert(number_of_rows_to_insert, 0, false, false, delay);
    }

    private void insertWithoutAutoCommitWithExplicitCommit(final int number_of_rows_to_insert, final long delay) throws SQLException {

        insertWithoutAutoCommitWithExplicitCommit(number_of_rows_to_insert, 0, delay);
    }

    private void insertWithoutAutoCommitWithExplicitCommit(final int number_of_rows_to_insert, final int starting_value, final long delay) throws SQLException {

        doInsert(number_of_rows_to_insert, starting_value, false, true, delay);
    }

    private void createWithAutoCommit() throws SQLException {

        doCreate(true);
    }

    private void createWithoutAutoCommit() throws SQLException {

        doCreate(false);
    }

    private void doCreate(final boolean auto_commit) throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                connection.setAutoCommit(auto_commit);
                createTable(connection);
            }
        });
    }

    private void doInsert(final int number_of_rows_to_insert, final int starting_value, final boolean auto_commit, final boolean explicit_commit, final long delay) throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                connection.setAutoCommit(auto_commit);
                insertValues(number_of_rows_to_insert, starting_value, connection, delay);

                if (explicit_commit) {
                    connection.commit();
                }
            }
        });
    }

    private void assertDataIsPresent(final int number_of_rows_inserted) throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                assertDataIsPresent(number_of_rows_inserted, connection);
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

    private void insertValues(final int number_of_rows_to_insert, final int starting_value, final Connection connection, final long delay) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            for (int i = 0; i < number_of_rows_to_insert; i++) {

                final int val = i + starting_value;

                statement.executeUpdate("INSERT INTO TEST VALUES(" + val + ");");
                sleep(delay);
            }
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void sleep(final long delay) {

        try {
            Thread.sleep(delay);
        }
        catch (final InterruptedException e) {
            // Ignore and carry on.
        }
    }

    private void assertDataIsPresent(final int number_of_rows_inserted, final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            // Check for duplicates.
            final Set<Integer> already_seen = new HashSet<Integer>();

            for (int i = 0; i < number_of_rows_inserted; i++) {

                // There should be another value.
                assertTrue(result_set.next());
                final int value_read = result_set.getInt(1);

                // The value shouldn't have been read already.
                assertFalse(already_seen.contains(value_read));

                // The value should be between 0 and n-1.
                assertTrue(value_read >= 0 && value_read < number_of_rows_inserted);

                already_seen.add(value_read);
            }
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
