package org.h2o.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.h2o.H2O;

public class TestDriver {

    protected Connection connection;
    protected long delay;

    public TestDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        delay = 0;
        final String jdbcURL = H2O.createDatabaseURL(db_port, database_base_directory_path, database_name);

        // Create connection to the H2O database instance.
        while (connection == null) {

            try {
                connection = DriverManager.getConnection(jdbcURL, username, password);
                connections_to_be_closed.add(connection);
            }
            catch (final SQLException e) {
                // Wait and retry.
                sleep(2000);
            }
        }
    }

    public void setAutoCommitOn() throws SQLException {

        connection.setAutoCommit(true);
    }

    public void setAutoCommitOff() throws SQLException {

        connection.setAutoCommit(false);
    }

    public void setNoDelay() {

        delay = 0;
    }

    public void setDelay(final long delay) {

        this.delay = delay;
    }

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

    protected void closeIfNotNull(final Statement statement) {

        try {
            if (statement != null) {
                statement.close();
            }
        }
        catch (final SQLException e) {
            // Ignore and carry on, only trying to tidy up.
        }
    }

    protected void sleep(final long delay) {

        try {
            Thread.sleep(delay);
        }
        catch (final InterruptedException e) {
            // Ignore and carry on.
        }
    }

    public void commit() throws SQLException {

        connection.commit();
    }

    public Connection getConnection() {

        return connection;
    }
}
