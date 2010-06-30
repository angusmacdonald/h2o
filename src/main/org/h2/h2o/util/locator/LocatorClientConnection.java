package org.h2.h2o.util.locator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.h2.h2o.util.locator.messages.ReplicaLocationsResponse;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorClientConnection {

	/**
	 * Socket used to connect to the locator server.
	 */
	private Socket s;

	/**
	 * Port on which the locator server can be found.
	 */
	private int port;

	/**
	 * Host on which the locator server can be found
	 */
	private String hostname;

	protected LocatorClientConnection(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
	}

	/**
	 * Create a new socket connection to the locator server.
	 * @throws IOException	Thrown if the locator server cannot be found.
	 */
	private void setupSocketConnection() throws IOException{
		try {
			s = new Socket(hostname, port);
		} catch (Exception e) {
			throw new IOException("Locator server was not found. Make sure server is running on " + hostname + ":" + port + ". Program will now terminate (original error: " + e.getMessage() + ").");
		}
	}

	/**
	 * If the socket is not currently conencted this method will try to make another connection. If this fails
	 * it will inform the calling method by returning false.
	 */
	public boolean checkIsConnected(){
		if (s == null || !s.isConnected()){
			try {
				setupSocketConnection();
			} catch (IOException e) {
				ErrorHandling.errorNoEvent("Locator server not found at " + hostname + ":" + port);
			}
		}

		return s != null && s.isConnected();
	}

	/*
	 * 
	 * METHODS THAT SEND MESSAGES TO THE SERVER.
	 * 
	 */

	/**
	 *  Send the current set of database instances which hold System Table meta-data state.
	 */
	public boolean sendDatabaseLocation(String[] locations) throws IOException{
		boolean successful = false;

		setupSocketConnection();
		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = LocatorProtocol.constructSetRequest(locations);

			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			int response = br.read();
			successful = (response == 1);

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null) os.close();
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return successful;
	}

	/**
	 * Request a lock on the locator server.
	 * @param databaseInstanceString	The local database instance. Used for logging purposes.
	 * @return	If greater than zero this is the update count. Zero if the lock wasn't taken out.
	 * @throws IOException
	 */
	public int requestLock(String databaseInstanceString) throws IOException{
		setupSocketConnection();

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = LocatorProtocol.constructLockRequest(databaseInstanceString);

			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			int result = br.read();
			return result;

		} finally {
			try {
				if (os != null) os.close();
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Confirm that the system table has been created successfully.
	 * @param databaseInstanceString	The local database instance. Used for logging purposes.
	 * @return	True if the commit action was confirmed; otherwise false.
	 * @throws IOException
	 */
	public boolean confirmSystemTableCreation(String databaseInstanceString)  throws IOException{
		setupSocketConnection();

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = LocatorProtocol.constructCommitRequest(databaseInstanceString);

			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			int result = br.read();

			return result == 1;

		} finally {
			try {
				if (os != null) os.close();
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * @param requestMessage
	 * @return
	 * @throws IOException
	 */
	private OutputStream writeToOutputStream(String requestMessage)
	throws IOException {
		OutputStream os;
		os = s.getOutputStream();

		os.write(requestMessage.getBytes());
		os.flush();
		return os;
	}

	public ReplicaLocationsResponse getDatabaseLocations() throws IOException{
		setupSocketConnection();
		List<String> locations = new LinkedList<String>();
		ReplicaLocationsResponse response = null;

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = LocatorProtocol.constructGetRequest();


			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String inputLine;

			int updateCount = Integer.parseInt(br.readLine());

			while ((inputLine = br.readLine()) != null) {
				locations.add(inputLine);
			}


			response = new ReplicaLocationsResponse(locations, updateCount);
		} finally {
			try {
				if (os != null) os.close();
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return response;
	}


	public static void main(String[] args) throws InterruptedException {
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		LocatorClientConnection lcc = new LocatorClientConnection("eigg", 29999);
		List<String> ls;
		try {
			ls = lcc.getDatabaseLocations().getLocations();


			for (String l: ls){
				System.out.println(l);
			}

			String[] locations = {"one1", "two1", "three1"};

			lcc.sendDatabaseLocation(locations);

			ls = lcc.getDatabaseLocations().getLocations();

			for (String l: ls){
				System.out.println(l);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
