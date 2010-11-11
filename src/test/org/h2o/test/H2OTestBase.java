package org.h2o.test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.junit.After;
import org.junit.Before;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

public abstract class H2OTestBase {

    private static final String DATABASE_NAME = "db";
    private static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data_";
    private static final String CONFIG_DIRECTORY_ROOT = "db_config_";
    private static final int LOWEST_PORT = 5000;
    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    // The diagnostic level set for both test and database processes. 0 is FULL, 6 is NONE.
    private static final int DIAGNOSTIC_LEVEL = 0;

    private Process locator_process;
    protected Process[] db_processes;
    private String[] database_base_directory_paths;
    private String config_directory_path;
    private PersistentStateManager persistent_state_manager;

    private static int first_locator_port = LOWEST_PORT + new Random().nextInt(20000);
    private static int first_db_port = first_locator_port + 20000;

    private Set<Connection> connections_to_be_closed;

    protected abstract int getNumberOfDatabases();

    public ITestDriverFactory getTestDriverFactory() {

        return new ITestDriverFactory() {

            @Override
            public TestDriver makeConnectionDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

                return new TestDriver(db_port, database_base_directory_path, database_name, username, password, connections_to_be_closed);
            }
        };
    }

    /**
     * Sets up the test.
     * 
     * @throws SQLException if fixture setup fails
     * @throws IOException if fixture setup fails
     * @throws UnknownPlatformException 
     * @throws UndefinedDiagnosticLevelException 
     */
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException {

        Diagnostic.setLevel(DiagnosticLevel.fromNumericalValue(DIAGNOSTIC_LEVEL));

        setUpConfigDirectoryPath();
        setUpDatabaseDirectoryPaths();

        persistent_state_manager = new PersistentStateManager(config_directory_path, database_base_directory_paths);

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

        first_locator_port++;
        first_db_port += getNumberOfDatabases();
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

    protected void startup() throws IOException, UnknownPlatformException {

        final String descriptor_file_path = startupLocator();

        startupDatabases(descriptor_file_path);
        connections_to_be_closed = new HashSet<Connection>();
    }

    private String startupLocator() throws UnknownPlatformException, IOException {

        final List<String> locator_args = new ArrayList<String>();
        locator_args.add("-n" + DATABASE_NAME);
        locator_args.add("-p" + first_locator_port);
        locator_args.add("-d");
        locator_args.add("-f" + config_directory_path);

        locator_process = ProcessInvocation.runJavaProcess(H2OLocator.class, locator_args);

        return config_directory_path + File.separator + DATABASE_NAME + ".h2od";
    }

    private void startupDatabases(final String descriptor_file_path) throws IOException, UnknownPlatformException {

        db_processes = new Process[database_base_directory_paths.length];

        for (int i = 0; i < db_processes.length; i++) {

            final int port = first_db_port + i;

            final List<String> db_args = new ArrayList<String>();
            db_args.add("-n" + DATABASE_NAME);
            db_args.add("-p" + port);
            db_args.add("-f" + database_base_directory_paths[i]);
            db_args.add("-d" + descriptor_file_path);
            db_args.add("-D" + DIAGNOSTIC_LEVEL);

            db_processes[i] = ProcessInvocation.runJavaProcess(H2O.class, db_args);
        }
    }

    void shutdown() {

        closeConnections();

        for (final Process p : db_processes) {
            p.destroy();
        }

        locator_process.destroy();
    }

    private void closeConnections() {

        for (final Connection connection : connections_to_be_closed) {
            closeIfNotNull(connection);
        }
    }

    protected TestDriver makeTestDriver() {

        return makeTestDriver(0);
    }

    protected TestDriver makeTestDriver(final int db_index) {

        final int db_port = first_db_port + db_index;
        return getTestDriverFactory().makeConnectionDriver(db_port, database_base_directory_paths[db_index], DATABASE_NAME, USER_NAME, PASSWORD, connections_to_be_closed);
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
