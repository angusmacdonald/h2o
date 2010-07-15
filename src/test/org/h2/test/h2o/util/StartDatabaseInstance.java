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
package org.h2.test.h2o.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.h2.h2o.manager.PersistentSystemTable;
import org.h2.tools.Server;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class StartDatabaseInstance extends Thread {

	private String connectionString;
	private Connection connection;


	private boolean running = true;
	private boolean createConnectionInSeperateThread;
	private Server server;
	private String port;


	public static void main(String[] args){

		Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

		String databaseConnectionString = arguments.get("-l");
		String port = arguments.get("-p");
		StartDatabaseInstance instance = new StartDatabaseInstance(databaseConnectionString, port, true);
		instance.run(); //this isn't being run in a seperate thread when called from here.
	}

	/**
	 * @param connectionString
	 */
	public StartDatabaseInstance(String connectionString, boolean createConnectionInSeperateThread) {

		if (!createConnectionInSeperateThread){
			try {
				this.connection = DriverManager.getConnection(connectionString, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		this.createConnectionInSeperateThread = createConnectionInSeperateThread;
		this.connectionString = connectionString;
	}

	public StartDatabaseInstance(String databaseConnectionString, String port, boolean b) {
		this(databaseConnectionString, b);
		this.port = port;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		if (createConnectionInSeperateThread){
			try {
				server = Server.createTcpServer(new String[] { "-tcpPort", port, connectionString });

				server.start();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}

			try {
				this.connection = DriverManager.getConnection(connectionString, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		while (isRunning()){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/*
		 * Shutdown.
		 */

		try {
			if (connection != null) connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection(){
		return connection;
	}

	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running;
	}

	/**
	 * @param running the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

	/**
	 * @return
	 */
	public boolean isConnected() {
		try {
			return (connection!=null && !connection.isClosed());
		} catch (SQLException e) {
			return false;
		}
	}
}
