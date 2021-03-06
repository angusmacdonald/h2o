/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.h2o.locator.LocatorProtocol;
import org.h2o.locator.messages.LockRequestResponse;
import org.h2o.locator.messages.ReplicaLocationsResponse;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Handles incoming connections from client databases looking to access (for read or write) the locator file.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorWorker extends Thread {

    private static final String SEPARATOR = "::";

    private final Socket socket;

    private final LocatorState locatorState;

    /**
     * @param newConnection
     *            The new incoming connection on the server.
     * @param locatorFile
     *            The location of the locator file, which stores where
     */
    protected LocatorWorker(final Socket newConnection, final LocatorState locatorFile) {

        locatorState = locatorFile;
        socket = newConnection;
    }

    /**
     * Service the current incoming connection.
     */
    @Override
    public void run() {

        try {
            try { // ends with 'finally' to close the socket connection.
                  // Diagnostic.traceNoEvent(DiagnosticLevel.INIT,
                  // "Created new LocatorConnectionHandler thread.");
                socket.setSoTimeout(5000);

                // Get single-line request from the client.

                final BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String requestLine = "";

                StringBuilder request = new StringBuilder();
                final String requestType = requestLine = br.readLine(); // The first line always specifies the type of the request being made.

                if (requestType == null) { // Request didn't contain anything.
                    return;
                }

                while ((requestLine = br.readLine()) != null) {
                    if (requestLine.contains("END")) {
                        break;
                    }

                    request.append(requestLine);
                    request.append(SEPARATOR);
                }

                if (request.length() > SEPARATOR.length()) {
                    request = new StringBuilder(request.substring(0, request.length() - SEPARATOR.length()));
                }
                /*
                 * If the request is empty this is interpreted as a request for the database locations. Read from the locator file and
                 * return this list. If the list does contain some text then this is a new set of database instance locations which hold
                 * system table state. Write these to the locator file
                 */

                if (requestType.equals(LocatorProtocol.GET)) {
                    final ReplicaLocationsResponse response = locatorState.readLocationsFromFile();
                    sendResponse(LocatorProtocol.constructGetResponse(response));
                }
                else if (requestType.equals(LocatorProtocol.SET)) {
                    final String[] databaseLocations = request.toString().split(SEPARATOR);
                    sendResponse(locatorState.writeLocationsToFile(databaseLocations));
                }
                else if (requestType.equals(LocatorProtocol.LOCK)) {
                    final LockRequestResponse response = locatorState.lock(request.toString());
                    sendResponse(LocatorProtocol.constructLockResponse(response));
                }
                else if (requestType.equals(LocatorProtocol.COMMIT)) {
                    sendResponse(locatorState.releaseLockOnFile(request.toString()));
                }
                else {
                    ErrorHandling.errorNoEvent("Request not recognized: " + requestType);
                }
            }
            finally {
                socket.close();
            }
        }
        catch (final IOException e) {

        }
    }

    /**
     * Send a response (the parameter of this method) to the client connected on the socket connection.
     * 
     * @param response
     *            The response to be sent.
     */
    private void sendResponse(final String response) throws IOException {

        final OutputStream output = socket.getOutputStream();
        output.write(response.getBytes());
        output.flush();
        output.close();
    }

    private void sendResponse(final int response) throws IOException {

        final OutputStream output = socket.getOutputStream();
        output.write(response);
        output.flush();
        output.close();
    }

    private void sendResponse(final boolean successful) throws IOException {

        final OutputStream output = socket.getOutputStream();
        output.write(successful ? 1 : 0);
        output.flush();
        output.close();
    }
}
