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

public abstract class H2OTestBase {

    private static final String DATABASE_NAME = "db";
    private static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data_";
    private static final String CONFIG_DIRECTORY_ROOT = "db_config_";
    private static final int FIRST_TCP_PORT = 9999;
    private static final int FIRST_LOCATOR_PORT = 5999;
    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    private H2OLocator locator;
    protected H2O[] dbs;
    private Connection[] connections;
    private String[] database_base_directory_paths;
    private int[] tcp_ports;
    private String config_directory_path;
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

        setUpConfigDirectoryPath();
        setUpDatabaseDirectoryPaths();
        setUpTcpPorts();

        persistent_state_manager = new PersistentStateManager(config_directory_path, database_base_directory_paths);

        startup();
    }

    private void setUpConfigDirectoryPath() {

        config_directory_path = CONFIG_DIRECTORY_ROOT + System.currentTimeMillis();
    }

    private void setUpDatabaseDirectoryPaths() {

        final long current = System.currentTimeMillis();
        database_base_directory_paths = new String[getNumberOfDatabases()];

        for (int i = 0; i < database_base_directory_paths.length; i++) {
            database_base_directory_paths[i] = DATABASE_BASE_DIRECTORY_ROOT + (current + i);
        }
    }

    private void setUpTcpPorts() {

        tcp_ports = new int[getNumberOfDatabases()];
    }

    protected abstract int getNumberOfDatabases();

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

    void startup() throws IOException, SQLException {

        final String descriptor_file_path = startupLocator();

        startupDatabases(descriptor_file_path);
        createConnections();
    }

    private void createConnections() throws SQLException {

        connections = new Connection[dbs.length];
        for (int i = 0; i < connections.length; i++) {
            connections[i] = makeConnection(i);
        }
    }

    private String startupLocator() {

        int locator_port = FIRST_LOCATOR_PORT;
        String descriptor_file_path;

        while (true) {
            try {
                locator = new H2OLocator(DATABASE_NAME, locator_port, true, config_directory_path);
                descriptor_file_path = locator.start();
                break;
            }
            catch (final IOException e) {
                locator_port++;
            }
        }
        return descriptor_file_path;
    }

    private void startupDatabases(final String descriptor_file_path) throws IOException {

        int tcp_port = FIRST_TCP_PORT;

        dbs = new H2O[database_base_directory_paths.length];

        for (int i = 0; i < dbs.length; i++) {
            while (true) {
                try {
                    dbs[i] = new H2O(DATABASE_NAME, tcp_port, database_base_directory_paths[i], descriptor_file_path);
                    tcp_ports[i] = tcp_port;
                    System.out.println("started db with port: " + tcp_port);

                    dbs[i].startDatabase();
                    break;
                }
                catch (final SQLException e) {
                    tcp_port++;
                }
            }
            tcp_port++;
        }
    }

    void shutdown() throws SQLException {

        closeConnections();

        for (final H2O db : dbs) {
            db.shutdown();
        }

        locator.shutdown();
    }

    private void closeConnections() throws SQLException {

        if (connections != null) {
            for (final Connection connection : connections) {
                closeIfNotNull(connection);
            }
        }
    }

    protected Connection[] getConnections() {

        return connections;
    }

    protected Connection makeConnection(final int db_index) throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + tcp_ports[db_index] + "/" + database_base_directory_paths[db_index] + "/" + DATABASE_NAME + tcp_ports[db_index];

        // Create connection to the H2O database instance.
        return DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);
    }

    protected void closeIfNotNull(final Connection connection) {

        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (final SQLException e) {
            // Ignore and carry on, only trying to tidy up.
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
}
