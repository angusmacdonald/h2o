package org.h2.test.h2o;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.h2o.manager.PersistentSystemTable;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseThread extends Thread {

	private String connectionString;
	private Connection connection;


	private boolean running = true;
	private boolean createConnectionInSeperateThread;

	/**
	 * @param connectionString
	 */
	public DatabaseThread(String connectionString, boolean createConnectionInSeperateThread) {

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

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		if (createConnectionInSeperateThread){
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
				// TODO Auto-generated catch block
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
