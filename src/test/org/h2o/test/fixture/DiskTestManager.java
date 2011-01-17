package org.h2o.test.fixture;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.h2o.H2O;

import uk.ac.standrews.cs.nds.remote_management.ProcessManager;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;

import com.mindbright.ssh2.SSH2Exception;

public class DiskTestManager extends TestManager {

    protected static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data_";

    private Process[] db_processes;
    private final IDiskConnectionDriverFactory connection_driver_factory;

    public DiskTestManager(final int number_of_databases, final IDiskConnectionDriverFactory connection_driver_factory) {

        super(number_of_databases);
        this.connection_driver_factory = connection_driver_factory;
    }

    @Override
    public void tearDown() throws SQLException {

        shutdown();

        super.tearDown();
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public void startup() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        super.startup();

        startupLocator();
        setupDatabaseDescriptorLocation();
        startupDatabaseProcesses();
    }

    @Override
    public void shutdown() {

        super.shutdown();

        shutdownDatabases();
        shutdownLocator();
    }

    @Override
    public ConnectionDriver makeConnectionDriver(final int db_index) {

        final int db_port = first_db_port + db_index;

        final ConnectionDriver makeConnectionDriver = connection_driver_factory.makeConnectionDriver(db_port, database_base_directory_paths[db_index], DATABASE_NAME_ROOT, USER_NAME, PASSWORD, connections_to_be_closed);

        return makeConnectionDriver;
    }

    public void killDBProcess(final int i) {

        db_processes[i].destroy();
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    protected void setUpDatabaseDirectoryPaths() {

        final long current = System.currentTimeMillis();
        database_base_directory_paths = new String[number_of_databases];

        for (int i = 0; i < database_base_directory_paths.length; i++) {
            database_base_directory_paths[i] = DATABASE_BASE_DIRECTORY_ROOT + (current + i);
        }
    }

    @Override
    protected boolean failIfPersistentStateCannotBeDeleted() {

        // Should be able to delete persistent state for on-disk databases.
        return true;
    }

    // -------------------------------------------------------------------------------------------------------

    private void startupDatabaseProcesses() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        db_processes = new Process[database_base_directory_paths.length];

        for (int i = 0; i < db_processes.length; i++) {

            final List<String> db_args = new ArrayList<String>();

            final int port = first_db_port + i;

            db_args.add("-n" + DATABASE_NAME_ROOT);
            db_args.add("-p" + port);
            db_args.add("-f" + database_base_directory_paths[i]);
            db_args.add("-d" + descriptor_file_path);
            db_args.add("-D" + DIAGNOSTIC_LEVEL.numericalValue());

            db_processes[i] = new ProcessManager().runJavaProcess(H2O.class, db_args);

            // TODO read the actual port used from the properties file.
        }
    }

    private void shutdownDatabases() {

        for (final Process p : db_processes) {
            p.destroy();
            try {
                p.waitFor();
            }
            catch (final InterruptedException e) {
                // Ignore.
            }
        }
    }
}
