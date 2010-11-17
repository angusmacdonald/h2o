package org.h2o.test.fixture;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.h2o.H2O;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

public class DiskTestManager extends TestManager {

    private Process[] db_processes;
    private final IDiskConnectionDriverFactory connection_driver_factory;

    public DiskTestManager(final int number_of_databases, final IDiskConnectionDriverFactory connection_driver_factory) {

        super(number_of_databases);
        this.connection_driver_factory = connection_driver_factory;
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

        Diagnostic.setLevel(DIAGNOSTIC_LEVEL);

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
    @Override
    public void tearDown() throws SQLException {

        shutdown();

        persistent_state_manager.deletePersistentState();

        first_locator_port++;
        first_db_port += number_of_databases;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public void startup() throws IOException, UnknownPlatformException {

        startupLocator();

        descriptor_file_path = getDatabaseDescriptorLocation();

        connections_to_be_closed = new HashSet<Connection>();

        startupDatabaseProcesses(descriptor_file_path);
    }

    @Override
    public void shutdown() {

        closeConnections();

        shutdownDatabases();
        shutdownLocator();

    }

    @Override
    protected void setUpDatabaseDirectoryPaths() {

        final long current = System.currentTimeMillis();
        database_base_directory_paths = new String[number_of_databases];

        for (int i = 0; i < database_base_directory_paths.length; i++) {
            database_base_directory_paths[i] = DATABASE_BASE_DIRECTORY_ROOT + (current + i);
        }
    }

    private void startupDatabaseProcesses(final String descriptor_file_path) throws IOException, UnknownPlatformException {

        db_processes = new Process[database_base_directory_paths.length];

        for (int i = 0; i < db_processes.length; i++) {

            final List<String> db_args = new ArrayList<String>();

            final int port = first_db_port + i;

            db_args.add("-n" + DATABASE_NAME);
            db_args.add("-p" + port);
            db_args.add("-f" + database_base_directory_paths[i]);
            db_args.add("-d" + descriptor_file_path);
            db_args.add("-D" + DIAGNOSTIC_LEVEL.numericalValue());

            db_processes[i] = ProcessInvocation.runJavaProcess(H2O.class, db_args);
        }
    }

    private void shutdownDatabases() {

        for (final Process p : db_processes) {
            p.destroy();
        }
    }

    @Override
    public ConnectionDriver makeConnectionDriver(final int db_index) {

        final int db_port = first_db_port + db_index;
        return connection_driver_factory.makeConnectionDriver(db_port, database_base_directory_paths[db_index], DATABASE_NAME, USER_NAME, PASSWORD, connections_to_be_closed);

    }

    public void killDBProcess(final int i) {

        db_processes[i].destroy();
    }
}
