package org.h2.test.h2o;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.h2o.manager.PersistentSchemaManager;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseThread extends Thread {

	private String connectionString;
	private Connection connection;
	
	
	private boolean running = true;
	
	/**
	 * @param connectionString
	 */
	public DatabaseThread(String connectionString) {
		
		try {
			this.connection = DriverManager.getConnection(connectionString, PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		this.connectionString = connectionString;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {

		
		while (isRunning()){}
		
		/*
		 * Shutdown.
		 */
		
		try {
			connection.close();
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
}
