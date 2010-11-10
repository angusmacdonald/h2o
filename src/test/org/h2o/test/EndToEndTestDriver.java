package org.h2o.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class EndToEndTestDriver extends TestDriver {

    public EndToEndTestDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        super(db_port, database_base_directory_path, database_name, username, password, connections_to_be_closed);
    }

    @Override
    public void createTable() throws SQLException {

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

    public void insertOneRow() {

        insertRows(1, 0, false);
    }

    public void insertRows(final int number_of_rows_to_insert, final int starting_value, final boolean commit) {

        while (true) {

            try {

                doInsert(number_of_rows_to_insert, starting_value);
                if (commit) {
                    commit();
                }
                break;
            }
            catch (final SQLException e) {
                // Ignore and try again.
            }
        }
    }

    private void doInsert(final int number_of_rows_to_insert, final int starting_value) throws SQLException {

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

    public void assertOneRowIsPresent() throws SQLException {

        assertDataIsPresent(1);
    }

    public void assertDataIsPresent(final int number_of_rows_inserted) throws SQLException {

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

    public void assertDataIsNotPresent() throws SQLException {

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

    public void insertRows(final int number_of_values) {

        insertRows(number_of_values, 0, false);
    }
}
