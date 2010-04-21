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
public class LocatorConnectionHandler extends Thread {

	private Socket clientSocket;
	private LocatorFileWriter locatorFile;

	/**
	 * @param newConnection
	 * @param locatorFile 
	 */
	public LocatorConnectionHandler(Socket newConnection, LocatorFileWriter locatorFile) {
		this.locatorFile = locatorFile;
		this.clientSocket = newConnection;
	}

	public void run(){
		try {

			try {
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Created new LocatorConnectionHandler thread.");

				//Get single-line request from the client.
				String clientRequest, entireRequest = "";
				BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));


				while ((clientRequest = br.readLine()) != null) {
					if (clientRequest.contains("END")){
						break;
					}

					entireRequest += clientRequest + "!!";

				}

				if (entireRequest.equals("")){
					//Send back database info
					Set<String> locations = locatorFile.read();

					String clientResponse = "";
					for (String s: locations){
						clientResponse += s + "\n";
					}
					OutputStream output = clientSocket.getOutputStream();
					output.write((clientResponse.getBytes()));
					output.flush();
					output.close();
				} else {
					//Update local file.
					String[] databaseLocations = entireRequest.split("!!");
					locatorFile.write(databaseLocations);

				}

			} finally {
				clientSocket.close();
			}


		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
