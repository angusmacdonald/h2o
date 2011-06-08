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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.h2o.H2O;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class StartDatabaseInstance extends Thread {

    private Connection connection;

    private boolean running = true;

    private final String databaseName;

    private final String databaseInstanceIdentifier;

    private final String databaseDirectoryPath;

    private final String databaseDescriptorLocation;

    public static void main(final String[] args) {

        Diagnostic.setLevel(DiagnosticLevel.FULL);

        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = arguments.get("-n");
        final String databaseInstanceIdentifier = arguments.get("-i");
        final String databaseDescriptorLocation = arguments.get("-d");
        final String databaseDirectoryPath = arguments.get("-p");

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Database Name: " + databaseName);
        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Instance Name: " + databaseInstanceIdentifier);
        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Directory Path: " + databaseDirectoryPath);
        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Descriptor Location: " + databaseDescriptorLocation);

        final StartDatabaseInstance instance = new StartDatabaseInstance(databaseName, databaseInstanceIdentifier, databaseDirectoryPath, databaseDescriptorLocation);
        instance.run(); // this isn't being run in a separate thread when called from here.
    }

    /**
     * @param connectionString
     */
    public StartDatabaseInstance(final String databaseName, final String databaseInstanceIdentifier, final String databaseDirectoryPath, final String databaseDescriptorLocation) {

        this.databaseDescriptorLocation = databaseDescriptorLocation;
        this.databaseName = databaseName;
        this.databaseInstanceIdentifier = databaseInstanceIdentifier;
        this.databaseDirectoryPath = databaseDirectoryPath;
    }

    @Override
    public void run() {

        final H2O newDatabase = new H2O(databaseName, databaseInstanceIdentifier, databaseDirectoryPath, databaseDescriptorLocation, DiagnosticLevel.FULL, 0);

        try {

            newDatabase.startDatabase();
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        while (isRunning()) {
            try {
                Thread.sleep(100);
            }
            catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        /*
         * Shutdown.
         */

        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (final SQLException e) {
        }
    }

    public Connection getConnection() {

        return connection;
    }

    /**
     * @return the running
     */
    public synchronized boolean isRunning() {

        return running;
    }

    /**
     * @param running
     *            the running to set
     */
    public synchronized void setRunning(final boolean running) {

        this.running = running;
    }

    /**
     * @return
     */
    public boolean isConnected() {

        try {
            return connection != null && !connection.isClosed();
        }
        catch (final SQLException e) {
            return false;
        }
    }
}
