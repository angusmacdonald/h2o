package org.h2.h2o.util.properties.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Handles incoming connections from client databases looking to access (for read or write) the locator file.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ServerWorkerThread extends Thread {

	private static final String SEPARATOR = "!!";
	private Socket socket;
	private LocatorFileWriter locatorFile;

	/**
	 * @param newConnection
	 * @param locatorFile 
	 */
	protected ServerWorkerThread(Socket newConnection, LocatorFileWriter locatorFile) {
		this.locatorFile = locatorFile;
		this.socket = newConnection;
	}

	public void run(){
		try {

			try {
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Created new LocatorConnectionHandler thread.");

				//Get single-line request from the client.
				String clientRequest, entireRequest = "";
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));


				while ((clientRequest = br.readLine()) != null) {
					if (clientRequest.contains("END")){
						break;
					}

					entireRequest += clientRequest + SEPARATOR;

				}

				/*
				 * If the request is empty this is interpreted as a request for the database locations. Read from the locator file
				 * and return this list.
				 * 
				 * If the list does contain some text then this is a new set of database instance locations which hold system table state.
				 * Write these to the locator file
				 */
				if (entireRequest.equals("")){
					//Send back database info
					Set<String> locations = locatorFile.readLocationsFromFile();

					String clientResponse = "";
					for (String s: locations){
						clientResponse += s + "\n";
					}

					OutputStream output = socket.getOutputStream();
					output.write((clientResponse.getBytes()));
					output.flush();
					output.close();
				} else if (entireRequest.startsWith("LOCK ")){
					boolean result = locatorFile.takeOutLockOnFile(entireRequest.substring("LOCK ".length()));
					
					returnResultOfLockRequest(result);
				} else if (entireRequest.startsWith("UNLOCK ")){
					boolean result = locatorFile.releaseLockOnFile(entireRequest.substring("UNLOCK ".length()));
					
					returnResultOfLockRequest(result);
				} else {

					//Update local file.
					String[] databaseLocations = entireRequest.split(SEPARATOR);
					locatorFile.writeLocationsToFile(databaseLocations);
				}

			} finally {
				socket.close();
			}


		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void returnResultOfLockRequest(boolean result) throws IOException {
		OutputStream output = socket.getOutputStream();
		output.write((result? 1:0));
		output.flush();
		output.close();
	}
}
