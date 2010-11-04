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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.junit.Test;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class FailingEndToEndTests {

    // The name of the database domain.
    private static final String DATABASE_NAME = "end_to_end";

    // Where the database will be created (where persisted state is stored).
    private static final String DATABASE_LOCATION = "db_data";

    // The port on which the database's TCP JDBC server will run.
    private static final int TCP_PORT = 9999;

    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    H2OLocator locator;
    H2O db;

    /**
     * Tests whether updates can be performed concurrently. The test starts two threads, each performing an update to the same table, with an artificial delay
     * to increase the probability of temporal overlap.
     * 
     * The test currently fails due to an "unexpected code path" error. When that is fixed the test should be changed to make the update threads retry on
     * error, since it's legitimate for an update to fail due to not being able to obtain locks.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void concurrentUpdates() throws SQLException, IOException {

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();

        createWithAutoCommit();

        final Semaphore sync = new Semaphore(-1);
        final SQLException[] exception_wrapper = new SQLException[1];

        final Thread firstUpdateThread = new UpdateThread(1, 0, 5000, sync, exception_wrapper);
        final Thread secondUpdateThread = new UpdateThread(1, 1, 5000, sync, exception_wrapper);

        firstUpdateThread.setName("First Update Thread");
        secondUpdateThread.setName("Second Update Thread");

        firstUpdateThread.start();
        secondUpdateThread.start();

        waitForThreads(sync);
        db.shutdown();
        if (exception_wrapper[0] != null) { throw exception_wrapper[0]; }

        db = initDB();
        db.startDatabase();
        assertDataIsPresent(2);
        db.shutdown();

        db.deletePersistentState();
    }

    /**
     * A generalised version of {@link #concurrentUpdates()} with multiple threads and multiple values being inserted.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void multipleThreads() throws SQLException, IOException {

        final int number_of_values = 10;
        final int number_of_threads = 5;

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();

        createWithAutoCommit();

        final ExecutorService pool = Executors.newFixedThreadPool(number_of_threads);
        final Semaphore sync = new Semaphore(1 - number_of_threads);
        final SQLException[] exception_wrapper = new SQLException[1];

        for (int i = 0; i < number_of_threads; i++) {

            final int j = i;

            pool.execute(new UpdateThread(number_of_values, j * number_of_values, 1000, sync, exception_wrapper));
        }

        waitForThreads(sync);
        db.shutdown();
        if (exception_wrapper[0] != null) { throw exception_wrapper[0]; }

        db = initDB();
        db.startDatabase();
        assertDataIsPresent(number_of_values * number_of_threads);
        db.shutdown();

        db.deletePersistentState();
    }

    // -------------------------------------------------------------------------------------------------------

    interface IDBAction {

        void execute(Connection connection) throws SQLException;
    }

    private class UpdateThread extends Thread {

        private final int number_of_values;
        private final int starting_value;
        private final long delay;
        private final Semaphore sync;
        private final SQLException[] exception_wrapper;

        public UpdateThread(final int number_of_values, final int starting_value, final long delay, final Semaphore sync, final SQLException[] exception_wrapper) {

            this.number_of_values = number_of_values;
            this.starting_value = starting_value;
            this.delay = delay;
            this.sync = sync;
            this.exception_wrapper = exception_wrapper;
        }

        @Override
        public void run() {

            try {
                insertWithoutAutoCommitWithExplicitCommit(number_of_values, starting_value, delay);
            }
            catch (final SQLException e) {
                exception_wrapper[0] = e;
            }
            finally {
                sync.release();
            }
        }
    };

    private void waitForThreads(final Semaphore sync) {

        while (true) {
            try {
                sync.acquire();
                break;
            }
            catch (final InterruptedException e) {
                // Try again.
            }
        }
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

    private void insertWithoutAutoCommitWithExplicitCommit(final int number_of_rows_to_insert, final int starting_value, final long delay) throws SQLException {

        doInsert(number_of_rows_to_insert, starting_value, false, true, delay);
    }

    private void createWithAutoCommit() throws SQLException {

        doCreate(true);
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
