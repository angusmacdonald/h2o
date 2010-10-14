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

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * The locator server class. Creates a ServerSocket and listens for connections constantly.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorServer extends Thread {
	
	private static final int LOCATOR_SERVER_PORT = 29999;
	
	private boolean running = true;
	
	private ServerSocket ss;
	
	private LocatorState locatorFile;
	
	private int port;
	
	private boolean finished = false;
	
	/**
	 * @param locatorServerPort
	 */
	public LocatorServer(int port, String databaseName) {
		this.port = port;
		locatorFile = new LocatorState("config" + File.separator + databaseName + port + ".locator");
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
				Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Server listening on port " + port + ", locator file at '" + locatorFile
						+ "'.");
				
			} catch ( IOException e ) {
				e.printStackTrace();
			}
			
			/*
			 * Start listening for incoming connections. Pass them off to a worker thread if they come.
			 */
			while ( isRunning() ) {
				try {
					
					Socket newConnection = ss.accept();
					
					LocatorWorker connectionHandler = new LocatorWorker(newConnection, locatorFile);
					connectionHandler.start();
				} catch ( IOException e ) {
					// e.printStackTrace();
				}
			}
		} finally {
			try {
				if ( ss != null )
					ss.close();
			} catch ( IOException e ) {
				e.printStackTrace();
			}
		}
		
		setFinished(true);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Diagnostic.setLevel(DiagnosticLevel.INIT);
		LocatorServer server = new LocatorServer(LOCATOR_SERVER_PORT, "locatorFile");
		server.start();
	}
	
	/**
	 * 
	 */
	public void createNewLocatorFile() {
		locatorFile.createNewLocatorFile();
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
