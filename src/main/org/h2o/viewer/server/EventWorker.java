/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.viewer.server;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.net.SocketException;

import org.h2o.viewer.client.H2OEvent;
import org.h2o.viewer.server.handlers.EventHandler;

/**
 * Handles incoming connections from client databases looking to access (for
 * read or write) the locator file.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class EventWorker extends Thread {

	private Socket socket;
	private EventHandler eventHandler;

	/**
	 * @param newConnection
	 *            The new incoming connection on the server.
	 * @param eventHandler
	 *            The location of the locator file, which stores where
	 */
	protected EventWorker(Socket newConnection, EventHandler eventHandler) {
		this.eventHandler = eventHandler;
		this.socket = newConnection;
	}

	/**
	 * Service the current incoming connection.
	 */
	public void run() {

		ObjectInputStream input = null;

		try {
			socket.setSoTimeout(5000);
			input = new ObjectInputStream(new BufferedInputStream(
					socket.getInputStream()));
		} catch (SocketException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			try { // ends with 'finally' to close the socket connection.
				// Diagnostic.traceNoEvent(DiagnosticLevel.INIT,
				// "Created new LocatorConnectionHandler thread.");

				// Get single-line request from the client.

				try {
					H2OEvent event = (H2OEvent) input.readObject();


					input.close();

					eventHandler.pushEvent(event);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

			} finally {
				socket.close();
			}
		} catch (IOException e) {

		}
	}
}
