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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.h2o.H2OLocator;

import uk.ac.standrews.cs.nds.remote_management.ProcessInvocation;
import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

/**
 * Base class for test managers that abstract over the details of instantiating and cleaning up a set of in-memory or on-disk database instances.
 * 
 * TODO explain port usage.
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

    protected static final String DATABASE_NAME_ROOT = "db";
    protected static final String DATABASE_BASE_DIRECTORY_ROOT = "db_data_";
    protected static final String CONFIG_DIRECTORY_ROOT = "db_config_";
    protected static final String USER_NAME = "sa";
    protected static final String PASSWORD = "";

    // The diagnostic level set for both test and database processes. 0 is FULL, 6 is NONE.
    protected static final DiagnosticLevel DIAGNOSTIC_LEVEL = DiagnosticLevel.NONE;

    private PersistentStateManager persistent_state_manager;
    private Process locator_process;
    private String config_directory_path;
    protected String descriptor_file_path;
    protected String[] database_base_directory_paths;
    protected int number_of_databases;
    protected Set<Connection> connections_to_be_closed;

    static int first_locator_port = LOWEST_PORT + new Random().nextInt(LOCATOR_PORT_RANGE);
    static int first_db_port = first_locator_port + LOCATOR_DB_PORT_SEPARATION;

    // -------------------------------------------------------------------------------------------------------

    public TestManager(final int number_of_databases) {

        this.number_of_databases = number_of_databases;
    }

    // -------------------------------------------------------------------------------------------------------

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

        persistent_state_manager.deletePersistentState();

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

    protected String getDatabaseDescriptorLocation() {

        return config_directory_path + File.separator + DATABASE_NAME_ROOT + DATABASE_DESCRIPTOR_SUFFIX;
    }

    protected void closeConnections() {

        for (final Connection connection : connections_to_be_closed) {
            closeIfNotNull(connection);
        }
    }

    protected void shutdownLocator() {

        if (locator_process != null) {
            locator_process.destroy();
        }
        locator_process = null;
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
