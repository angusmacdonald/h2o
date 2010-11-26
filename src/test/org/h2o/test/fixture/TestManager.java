/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2o.test.fixture;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2o.H2OLocator;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Base class for test managers that abstract over the details of instantiating and cleaning up a set of in-memory or on-disk database instances.
 * 
 * TODO explain port usage.
 * TODO explain database and directory naming.
 * TODO explain locator process.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public abstract class TestManager implements ITestManager {

    // The file suffix for database descriptor files.
    private static final String DATABASE_DESCRIPTOR_SUFFIX = ".h2od";

    // The lowest value that may be chosen for the locator port in a particular test.
    private static final int LOWEST_PORT = 5000;

    // The size of the range from which locator ports are chosen.
    private static final int LOCATOR_PORT_RANGE = 20000;

    // The separation between the chosen locator port and the first database port.
    private static final int LOCATOR_DB_PORT_SEPARATION = 20000;

    // The root for configuration directory names, to be appended with time-stamp.
    private static final String CONFIG_DIRECTORY_ROOT = "db_config_";

    // The root for database names, used differently by in-memory and on-disk test managers.
    protected static final String DATABASE_NAME_ROOT = "db";

    // The database user name.
    protected static final String USER_NAME = Constants.MANAGEMENT_DB_USER;

    // The database password.
    protected static final String PASSWORD = "";

    // The diagnostic level set for both test and database processes. 0 is FULL, 6 is NONE.
    protected static final DiagnosticLevel DIAGNOSTIC_LEVEL = DiagnosticLevel.NONE;

    // -------------------------------------------------------------------------------------------------------

    // The locator port used in the first test.
    static int first_locator_port = LOWEST_PORT + new Random().nextInt(LOCATOR_PORT_RANGE);

    // The first database port used in the first test.
    static int first_db_port = first_locator_port + LOCATOR_DB_PORT_SEPARATION;

    // -------------------------------------------------------------------------------------------------------

    // The persistent state manager used to clean up on-disk state on tear-down.
    private PersistentStateManager persistent_state_manager;

    // A handle to the locator server process.
    private Process locator_process;

    // The path of the directory within which the locator server creates the database descriptor file.
    private String config_directory_path;

    // The path of the database descriptor file.
    protected String descriptor_file_path;

    // The paths of the base directories within which the database files are stored.
    protected String[] database_base_directory_paths;

    // The number of databases used in the test.
    protected int number_of_databases;

    // The connections created in the test, to be closed on tear-down.
    protected Set<Connection> connections_to_be_closed;

    // -------------------------------------------------------------------------------------------------------

    /**
     * Initialises a test manager using a given number of database instances.
     * 
     * @param number_of_databases the number of databases
     */
    public TestManager(final int number_of_databases) {

        this.number_of_databases = number_of_databases;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public void setUp() throws IOException, UnknownPlatformException {

        Diagnostic.setLevel(DIAGNOSTIC_LEVEL);
        Diagnostic.setTimestampFlag(true);
        Diagnostic.setTimestampFormat(new SimpleDateFormat("HH:mm:ss:SSS "));
        Diagnostic.setTimestampDelimiterFlag(false);

        setUpConfigDirectoryPath();
        setUpDatabaseDirectoryPaths();

        persistent_state_manager = new PersistentStateManager(config_directory_path, database_base_directory_paths);
        startup();
    }

    @Override
    public void tearDown() throws SQLException {

        // In some cases we can't reliably delete persistent state due to lack of control
        // over when the database shuts down. So delegate the decision as to whether to fail to sub-classes.
        final boolean fail_if_persistent_state_cannot_be_deleted = failIfPersistentStateCannotBeDeleted();

        persistent_state_manager.deletePersistentState(fail_if_persistent_state_cannot_be_deleted);

        // Update ports ready for the next test.
        first_locator_port++;
        first_db_port += number_of_databases;
    }

    @Override
    public void startup() throws IOException, UnknownPlatformException {

        connections_to_be_closed = new HashSet<Connection>();
    }

    @Override
    public void shutdown() {

        closeConnections();
    }

    // -------------------------------------------------------------------------------------------------------

    protected abstract void setUpDatabaseDirectoryPaths();

    protected abstract boolean failIfPersistentStateCannotBeDeleted();

    // -------------------------------------------------------------------------------------------------------

    protected void startupLocator() throws UnknownPlatformException, IOException {

        if (locator_process == null) {

            final List<String> locator_args = new ArrayList<String>();
            locator_args.add("-n" + DATABASE_NAME_ROOT);
            locator_args.add("-p" + first_locator_port);
            locator_args.add("-d");
            locator_args.add("-f" + config_directory_path);
            locator_args.add("-D" + DIAGNOSTIC_LEVEL.numericalValue());

            locator_process = ProcessInvocation.runJavaProcess(H2OLocator.class, locator_args);
        }
    }

    protected void shutdownLocator() {

        if (locator_process != null) {
            locator_process.destroy();
        }
        locator_process = null;
    }

    protected void setupDatabaseDescriptorLocation() {

        descriptor_file_path = config_directory_path + File.separator + DATABASE_NAME_ROOT + DATABASE_DESCRIPTOR_SUFFIX;
    }

    protected void closeConnections() {

        for (final Connection connection : connections_to_be_closed) {
            closeIfNotNull(connection);
        }
    }

    // -------------------------------------------------------------------------------------------------------

    private void setUpConfigDirectoryPath() {

        config_directory_path = CONFIG_DIRECTORY_ROOT + System.currentTimeMillis();
    }

    private void closeIfNotNull(final Connection connection) {

        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (final SQLException e) {
            // Ignore and carry on, only trying to tidy up.
        }
    }
}
