package org.h2o.test.fixture;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.h2o.H2OLocator;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

public abstract class TestManager implements ITestManager {

    protected static final String DATABASE_NAME = "db";
    protected static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data_";
    protected static final String USER_NAME = "sa";
    protected static final String PASSWORD = "";

    // The diagnostic level set for both test and database processes. 0 is FULL, 6 is NONE.
    protected static final DiagnosticLevel DIAGNOSTIC_LEVEL = DiagnosticLevel.NONE;
    private static final String CONFIG_DIRECTORY_ROOT = "db_config_";

    protected String config_directory_path;
    protected String descriptor_file_path;
    protected PersistentStateManager persistent_state_manager;
    protected String[] database_base_directory_paths;
    protected int number_of_databases;

    private static final int LOWEST_PORT = 5000;
    protected static int first_locator_port = LOWEST_PORT + new Random().nextInt(20000);
    protected static int first_db_port = first_locator_port + 20000;
    private Process locator_process;
    protected Set<Connection> connections_to_be_closed;

    public TestManager(final int number_of_databases) {

        this.number_of_databases = number_of_databases;
    }

    /**
     * Sets up the test.
     * 
     * @throws SQLException if fixture setup fails
     * @throws IOException if fixture setup fails
     * @throws UnknownPlatformException 
     * @throws UndefinedDiagnosticLevelException 
     */
    @Override
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException {

    }

    /**
     * Tears down the test, removing persistent state.
     * 
     * @throws SQLException if fixture tear-down fails
     */
    @Override
    public void tearDown() throws SQLException {

    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public void startup() throws IOException, UnknownPlatformException {

    }

    @Override
    public void shutdown() {

    }

    protected void setUpConfigDirectoryPath() {

        config_directory_path = CONFIG_DIRECTORY_ROOT + System.currentTimeMillis();
    }

    protected abstract void setUpDatabaseDirectoryPaths();

    protected void startupLocator() throws UnknownPlatformException, IOException {

        if (locator_process == null) {

            final List<String> locator_args = new ArrayList<String>();
            locator_args.add("-n" + DATABASE_NAME);
            locator_args.add("-p" + first_locator_port);
            locator_args.add("-d");
            locator_args.add("-f" + config_directory_path);

            locator_process = ProcessInvocation.runJavaProcess(H2OLocator.class, locator_args);
        }
    }

    protected String getDatabaseDescriptorLocation() {

        return config_directory_path + File.separator + DATABASE_NAME + ".h2od";
    }

    protected void closeConnections() {

        for (final Connection connection : connections_to_be_closed) {
            closeIfNotNull(connection);
        }
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

    protected void shutdownLocator() {

        if (locator_process != null) {
            locator_process.destroy();
        }
        locator_process = null;
    }

    //    protected ITestDriverFactory getTestDriverFactory() {
    //
    //        return new ITestDriverFactory() {
    //
    //            @Override
    //            public ConnectionDriver makeConnectionDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {
    //
    //                return new ConnectionDriver(db_port, database_base_directory_path, database_name, username, password, connections_to_be_closed);
    //            }
    //
    //            @Override
    //            public ConnectionDriver makeConnectionDriver(final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {
    //
    //                return new ConnectionDriver(database_name, username, password, connections_to_be_closed);
    //            }
    //        };
    //    }
}
