/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator.server;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.h2o.util.LocalH2OProperties;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * The locator server class. Creates a ServerSocket and listens for connections constantly.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorServer extends Thread {

    private static final long SHUTDOWN_CHECK_DELAY = 2000;

    private boolean running = true;

    private final ServerSocket server_socket;

    private final LocatorState locatorState;

    private boolean finished = false;

    public LocatorServer(final int port, final String databaseName) throws IOException {

        this(port, databaseName, LocalH2OProperties.DEFAULT_CONFIG_DIRECTORY);
    }

    public LocatorServer(final int port, final String databaseName, final String locatorFileDirectory) throws IOException {

        locatorState = new LocatorState(locatorFileDirectory + File.separator + databaseName + port + ".locator");

        // Set up the server socket.
        server_socket = new ServerSocket(port);

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Server listening on port " + port + ", locator file at '" + locatorState + "'.");
    }

    /**
     * Starts the server and listens until the running field is set to false.
     */
    @Override
    public void run() {

        try {
            // Start listening for incoming connections. Pass them off to a worker thread if they come.
            while (true) {

                final Socket newConnection = server_socket.accept();

                final LocatorWorker connectionHandler = new LocatorWorker(newConnection, locatorState);
                connectionHandler.start();
            }
        }
        catch (final IOException e) {
            // Server socket has been closed.
        }

        setFinished(true);
    }

    public void shutdown() {

        try {
            if (server_socket != null) {
                server_socket.close();
            }
        }
        catch (final IOException e) {
            ErrorHandling.exceptionError(e, "Error closing server socket");
        }

        while (!isFinished()) {
            try {
                Thread.sleep(SHUTDOWN_CHECK_DELAY);
            }
            catch (final InterruptedException e) {
                // Ignore and carry on.
            }
        }
    }

    public void createNewLocatorFile() {

        locatorState.createNewLocatorFile();
    }

    public synchronized boolean isRunning() {

        return running;
    }

    public synchronized void setRunning(final boolean running) {

        this.running = running;
    }

    public synchronized boolean isFinished() {

        return finished;
    }

    public synchronized void setFinished(final boolean finished) {

        this.finished = finished;
    }
}
