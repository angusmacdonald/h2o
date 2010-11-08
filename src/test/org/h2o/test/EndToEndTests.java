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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTests extends H2OTestBase {

    /**
      * Tests whether a new database can be created, data inserted and read back.
      * 
      * @throws SQLException if the test fails
      * @throws IOException if the test fails
      */
    @Test
    public void simpleLifeCycle() throws SQLException, IOException {

        Diagnostic.trace();

        createWithAutoCommit();
        insertOneRowWithAutoCommitNoDelay();
        assertOneRowIsPresent();
    }

    /**
     * Tests whether a database is properly cleaned up, by running the whole life cycle twice.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void completeCleanUp() throws SQLException, IOException {

        Diagnostic.trace();

        simpleLifeCycle();
        tearDown();
        setUp();
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

        Diagnostic.trace();

        createWithAutoCommit();
        insertOneRowWithAutoCommitNoDelay();
        shutdown();

        startup();
        assertOneRowIsPresent();
    }

    /**
     * Tests whether data that has been inserted but not committed is visible within the same transaction.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void updateVisibleWithinTransaction() throws SQLException, IOException {

        Diagnostic.trace();

        createWithoutAutoCommit();
        insertOneRowNoCommitNoDelay();
        assertOneRowIsPresent();
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

        Diagnostic.trace();

        createWithoutAutoCommit();
        insertOneRowNoCommitNoDelay();
        shutdown();

        startup();
        assertDataIsNotPresent();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void explicitCommit() throws SQLException, IOException {

        Diagnostic.trace();

        createWithoutAutoCommit();
        insertOneRowExplicitCommitNoDelay();
        shutdown();

        startup();
        assertOneRowIsPresent();
    }

    /**
     * Tests whether a series of values can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void multipleInserts() throws SQLException, IOException {

        Diagnostic.trace();

        final int number_of_values = 100;

        createWithoutAutoCommit();
        insertRowsExplicitCommitNoDelay(number_of_values);
        shutdown();

        startup();
        assertDataIsPresent(number_of_values);
    }

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

        Diagnostic.trace();

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
        if (exception_wrapper[0] != null) { throw exception_wrapper[0]; }
        shutdown();

        startup();
        assertDataIsPresent(2);
    }

    /**
     * A generalised version of {@link #concurrentUpdates()} with multiple threads and multiple values being inserted.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void multipleThreads() throws SQLException, IOException {

        Diagnostic.trace();

        final int number_of_values = 10;
        final int number_of_threads = 10;

        createWithAutoCommit();

        final ExecutorService pool = Executors.newFixedThreadPool(number_of_threads);
        final Semaphore sync = new Semaphore(1 - number_of_threads);
        final SQLException[] exception_wrapper = new SQLException[1];

        for (int i = 0; i < number_of_threads; i++) {

            final int j = i;

            pool.execute(new UpdateThread(number_of_values, j * number_of_values, 1000, sync, exception_wrapper));
        }

        waitForThreads(sync);
        if (exception_wrapper[0] != null) { throw exception_wrapper[0]; }
        shutdown();

        startup();
        assertDataIsPresent(number_of_values * number_of_threads);
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

    private void doInsert(final int number_of_rows_to_insert, final int starting_value, final boolean auto_commit, final boolean explicit_commit, final long delay) {

        while (true) {

            try {
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
                break;
            }
            catch (final SQLException e) {
                // Ignore and try again.
            }
        }
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
}
