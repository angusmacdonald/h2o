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
package org.h2o.event.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.h2o.event.gui.BasicEventGui;
import org.h2o.event.server.handlers.EventFileWriter;
import org.h2o.event.server.handlers.EventHandler;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * The locator server class. Creates a ServerSocket and listens for connections
 * constantly.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class EventServer extends Thread {

	public static final int EVENT_SERVER_PORT = 4444;
	private boolean running = true;

	private ServerSocket ss;
	private EventHandler eventHandler;
	private int port;
	private boolean finished = false;

	/**
	 * @param locatorServerPort
	 */
	public EventServer(int port, EventHandler eventHandler) {
		this.port = port;
		this.eventHandler = eventHandler;
	}

	/**
	 * Starts the server and listens until the running field is set to false.
	 */
	public void run() {
		try {
			/*
			 * Set up the server socket.
			 */
			try {
				ss = new ServerSocket(port);

				ss.setSoTimeout(500);
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL,
						"Server listening on port " + port + ", event log at '"
								+ eventHandler + "'.");

			} catch (IOException e) {
				e.printStackTrace();
			}

			/*
			 * Start listening for incoming connections. Pass them off to a
			 * worker thread if they come.
			 */
			while (isRunning()) {
				try {

					Socket newConnection = ss.accept();
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL,
							"New connection from: "
									+ newConnection.getInetAddress()
											.getHostName() + "."
									+ newConnection.getPort());

					EventWorker connectionHandler = new EventWorker(newConnection, eventHandler);
					connectionHandler.start();
				} catch (IOException e) {
					// e.printStackTrace();
				}
			}
		} finally {
			try {
				if (ss != null)
					ss.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		setFinished(true);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Diagnostic.setLevel(DiagnosticLevel.FULL);
		EventServer server = new EventServer(EVENT_SERVER_PORT, new EventFileWriter("eventlog.txt"));
		server.start();
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
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

	/**
	 * @return the finished
	 */
	public synchronized boolean isFinished() {
		return finished;
	}

	/**
	 * @param finished
	 *            the finished to set
	 */
	public synchronized void setFinished(boolean finished) {
		this.finished = finished;
	}

}
