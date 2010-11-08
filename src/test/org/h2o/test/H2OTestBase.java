package org.h2o.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.junit.After;
import org.junit.Before;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class H2OTestBase {

    private static final String DATABASE_NAME = "db";
    private static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data";
    private static final int TCP_PORT = 9999;
    private static final int LOCATOR_PORT = 5999;
    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    private H2OLocator locator;
    private H2O db;
    private String database_base_directory_path;
    private PersistentStateManager persistent_state_manager;

    /**
     * Sets up the test.
     * 
     * @throws SQLException if fixture setup fails
     * @throws IOException if fixture setup fails
     */
    @Before
    public void setUp() throws SQLException, IOException {

        Diagnostic.setLevel(DiagnosticLevel.NONE);

        database_base_directory_path = DATABASE_BASE_DIRECTORY_ROOT + System.currentTimeMillis();
        persistent_state_manager = new PersistentStateManager(database_base_directory_path);

        startup();
    }

    /**
     * Tears down the test, removing persistent state.
     * 
     * @throws SQLException if fixture tear-down fails
     */
    @After
    public void tearDown() throws SQLException {

        shutdown();

        persistent_state_manager.deletePersistentState();
        persistent_state_manager.assertPersistentStateIsAbsent();
    }

    void startup() throws IOException {

        final String descriptor_file_path = startupLocator();

        startupDatabase(descriptor_file_path);
    }

    private String startupLocator() {

        int locator_port = LOCATOR_PORT;
        String descriptor_file_path;

        while (true) {
            try {
                locator = new H2OLocator(DATABASE_NAME, database_base_directory_path, locator_port, TCP_PORT, true);
                descriptor_file_path = locator.start();
                break;
            }
            catch (final IOException e) {
                locator_port++;
            }
        }
        return descriptor_file_path;
    }

    private void startupDatabase(final String descriptor_file_path) throws IOException {

        int tcp_port = TCP_PORT;

        while (true) {
            try {
                db = new H2O(DATABASE_NAME, TCP_PORT, database_base_directory_path, descriptor_file_path);

                db.startDatabase();
                break;
            }
            catch (final SQLException e) {
                tcp_port++;
            }
        }
    }

    void shutdown() throws SQLException {

        db.shutdown();
        locator.shutdown();
    }

    protected Connection makeConnection() throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + TCP_PORT + "/" + database_base_directory_path + "/" + DATABASE_NAME + TCP_PORT;

        // Create connection to the H2O database instance.
        return DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);
    }

    protected void closeIfNotNull(final Connection connection) throws SQLException {

        if (connection != null) {
            connection.close();
        }
    }

    protected void closeIfNotNull(final Statement statement) throws SQLException {

        if (statement != null) {
            statement.close();
        }
    }
}
