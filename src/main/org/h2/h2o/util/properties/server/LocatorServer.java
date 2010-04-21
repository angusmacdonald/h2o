package org.h2.h2o.util.properties.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * The locator server class. Creates a ServerSocket and listens for connections constantly.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorServer extends Thread{

	private static final int LOCATOR_SERVER_PORT = 29999;
	private boolean running = true;
	private ServerSocket ss;
	private LocatorFileWriter locatorFile;
	private int port;
	private boolean finished = false;
	
	/**
	 * @param locatorServerPort
	 */
	public LocatorServer(int port, String locatorFileLocation) {
		this.port = port;
		locatorFile = new LocatorFileWriter(locatorFileLocation);
	}


	public void run(){
		try {
			/*
			 * Set up the server socket.
			 */
			try {
				ss = new ServerSocket(LOCATOR_SERVER_PORT);
				
				ss.setSoTimeout(500);
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Server listening on port " + port);

			} catch (IOException e) {
				e.printStackTrace();
			}

			/*
			 * Start listening for incoming connections. Pass them off to a worker thread if they come.
			 */
			while (isRunning()){
				try {
					Socket newConnection = ss.accept();
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "New connection from: " + newConnection.getInetAddress().getHostName() + "." +  newConnection.getPort());

					ServerWorkerThread connectionHandler = new ServerWorkerThread(newConnection, locatorFile);
					connectionHandler.start();
				} catch (IOException e) {
					//e.printStackTrace();
				}
			}
		} finally {
			try {
				if (ss != null) ss.close();
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
	 * @param running the running to set
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
	 * @param finished the finished to set
	 */
	public synchronized void setFinished(boolean finished) {
		this.finished = finished;
	}

}
