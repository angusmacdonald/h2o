package org.h2o.test.fixture;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.DatabaseURL;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

public abstract class ConnectionDriver {

    protected Connection connection;
    protected long delay;

    public ConnectionDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        final DatabaseID jdbcURL = new DatabaseID(new DatabaseURL(db_port, database_base_directory_path, database_name));

        init(jdbcURL, username, password, connections_to_be_closed);
    }

    public ConnectionDriver(final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        final DatabaseID jdbcURL = new DatabaseID(new DatabaseURL(database_name));

        init(jdbcURL, username, password, connections_to_be_closed);
    }

    private void init(final DatabaseID jdbcURL, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        delay = 0;

        // Create connection to the H2O database instance.
        while (connection == null) {

            try {
                connection = DriverManager.getConnection(jdbcURL.getURL(), username, password);
                connections_to_be_closed.add(connection);
            }

            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Error connecting to database at: " + jdbcURL.getURL());
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

    protected void closeIfNotNull(final ResultSet result_set) {

        try {
            if (result_set != null) {
                result_set.close();
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
