package org.h2o.test.endtoend.fixture;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.h2o.test.fixture.ConnectionDriver;

public class EndToEndConnectionDriver extends ConnectionDriver {

    private static final int DEFAULT_NUMBER_OF_COLUMNS = 1;

    private int number_of_columns = DEFAULT_NUMBER_OF_COLUMNS;

    // -------------------------------------------------------------------------------------------------------

    public EndToEndConnectionDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        super(db_port, database_base_directory_path, database_name, username, password, connections_to_be_closed);
    }

    public EndToEndConnectionDriver(final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        super(database_name, username, password, connections_to_be_closed);
    }

    // -------------------------------------------------------------------------------------------------------

    public void createTable() throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Create a table.
            statement.executeUpdate(makeCreationStatement(false));
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    public void assertTableCantBeRecreated() {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Attempt to recreate a table, without guard.
            statement.executeUpdate(makeCreationStatement(false));
            fail("database TEST was created twice");
        }
        catch (final SQLException e) {
            // Expected.
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    public void assertCreateIfNotExistsSuccessful() throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Attempt to recreate a table, using guard.
            statement.executeUpdate(makeCreationStatement(true));
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    public void insertOneRow() {

        insertRows(1, 0, false);
    }

    public void insertRows(final int number_of_values) {

        insertRows(number_of_values, 0, false);
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
                // Filter out messages saying couldn't get locks.
                if (!e.getMessage().startsWith("Could")) {
                    System.out.println("retrying after exception: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    public void assertOneRowIsPresent() throws SQLException {

        assertDataIsCorrect(1);
    }

    public void assertDataIsCorrect(final int number_of_rows_expected) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            assertCorrectNumberOfRows(statement, number_of_rows_expected);
            assertValuesInRange(statement, number_of_rows_expected);
            assertNoDuplicateValues(statement, number_of_rows_expected); // Things are very bad if this fails - relational semantics.
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    public void assertTableIsNotPresent() throws SQLException {

        Statement statement = null;
        ResultSet result_set = null;
        try {
            statement = connection.createStatement();
            result_set = statement.executeQuery(makeSelectionStatement());

            fail("database TEST was present unexpectedly");
        }
        catch (final SQLException e) {
            // Expected path since TEST should not exist.
        }
        finally {
            closeIfNotNull(result_set);
            closeIfNotNull(statement);
        }
    }

    public void setNumberOfColumns(final int number_of_columns) {

        this.number_of_columns = number_of_columns;
    }

    // -------------------------------------------------------------------------------------------------------

    private void doInsert(final int number_of_rows_to_insert, final int starting_value) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            for (int i = 0; i < number_of_rows_to_insert; i++) {

                final int val = i + starting_value;

                statement.executeUpdate(makeInsertionStatement(val));
                sleep(delay);
            }
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void assertCorrectNumberOfRows(final Statement statement, final int number_of_rows_expected) throws SQLException {

        ResultSet result_set = null;

        try {
            result_set = statement.executeQuery(makeSelectionStatement());

            for (int i = 0; i < number_of_rows_expected; i++) {
                assertThat("expected another row", result_set.next(), is(true));
            }

            assertThat("expected " + number_of_rows_expected + " rows but found more", result_set.next(), is(false));
        }
        finally {
            closeIfNotNull(result_set);
        }
    }

    private void assertValuesInRange(final Statement statement, final int number_of_rows_expected) throws SQLException {

        ResultSet result_set = null;

        try {
            result_set = statement.executeQuery(makeSelectionStatement());

            for (int i = 0; i < number_of_rows_expected; i++) {

                result_set.next();

                // Get value of attribute with index 1.
                final int value_read = result_set.getInt(1);

                // The value should be between 0 and n-1.
                assertTrue(value_read >= 0 && value_read < number_of_rows_expected);
            }
        }
        finally {
            closeIfNotNull(result_set);
        }
    }

    private void assertNoDuplicateValues(final Statement statement, final int number_of_rows_expected) throws SQLException {

        ResultSet result_set = null;

        try {
            result_set = statement.executeQuery(makeSelectionStatement());

            final Set<Integer> already_seen = new HashSet<Integer>();

            for (int i = 0; i < number_of_rows_expected; i++) {

                result_set.next();
                final int value_read = result_set.getInt(1);

                // The value shouldn't have been read already.
                assertFalse(already_seen.contains(value_read));

                already_seen.add(value_read);
            }
        }
        finally {
            closeIfNotNull(result_set);
        }
    }

    private String makeCreationStatement(final boolean include_guard) {

        final StringBuilder builder = new StringBuilder();

        builder.append("CREATE TABLE " + (include_guard ? "IF NOT EXISTS " : "") + "TEST (");
        for (int i = 0; i < number_of_columns; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("ID");
            builder.append(i);
            builder.append(" INT");
        }
        builder.append(");");

        return builder.toString();
    }

    private String makeInsertionStatement(final int val) {

        final StringBuilder builder = new StringBuilder();

        builder.append("INSERT INTO TEST VALUES(");
        for (int i = 0; i < number_of_columns; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(val);
        }
        builder.append(");");

        return builder.toString();
    }

    private String makeSelectionStatement() {

        return "SELECT * FROM TEST;";
    }
}
