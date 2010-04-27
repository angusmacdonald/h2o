package org.h2.h2o.util.properties.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorClientConnection {

	private Socket s;
	private int port;
	private String hostname;

	protected LocatorClientConnection(String hostname, int port){
		this.hostname = hostname;
		this.port = port;
	}

	private void setupSocketConnection() throws IOException{
		try {
			s = new Socket(hostname, port);
		} catch (UnknownHostException e) {
			throw new IOException("Locator server was not found. Make sure server is running on " + hostname + ":" + port + ". Program will now terminate (original error: " + e.getMessage() + ").");
		} catch (IOException e) {
			throw new IOException("Locator server was not found. Make sure server is running on " + hostname + ":" + port + ". Program will now terminate (original error: " + e.getMessage() + ").");
		}
	}

	public void sendDatabaseLocation(String[] locations) throws IOException{
		setupSocketConnection();
		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = "";

			for (String location: locations){
				requestMessage += location + "\n";
			}

			requestMessage += "END\n";

			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String inputLine;

			//initiate conversation with client
			while ((inputLine = br.readLine()) != null) {
				System.out.println(inputLine);
			}
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
	}

	public boolean lockLocator(String databaseInstanceString) throws IOException{
		setupSocketConnection();

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = "LOCK " + databaseInstanceString + "\n";
			requestMessage += "END\n";

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

	public boolean unlockLocator(String databaseInstanceString)  throws IOException{
		setupSocketConnection();

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = "UNLOCK " + databaseInstanceString + "\n";
			requestMessage += "END\n";
			
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

	public Set<String> getDatabaseLocations() throws IOException{
		setupSocketConnection();
		Set<String> locations = new HashSet<String>();

		OutputStream os = null;
		BufferedReader br = null;
		try {
			String requestMessage = "END\n";


			os = writeToOutputStream(requestMessage);

			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String inputLine;

			//initiate conversation with client

			while ((inputLine = br.readLine()) != null) {
				locations.add(inputLine);
			}

		} finally {
			try {
				if (os != null) os.close();
				if (br != null) br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return locations;
	}


	public static void main(String[] args) throws InterruptedException {
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		LocatorClientConnection lcc = new LocatorClientConnection("eigg", 29999);
		Set<String> ls;
		try {
			ls = lcc.getDatabaseLocations();


			for (String l: ls){
				System.out.println(l);
			}

			String[] locations = {"one1", "two1", "three1"};

			lcc.sendDatabaseLocation(locations);

			ls = lcc.getDatabaseLocations();

			for (String l: ls){
				System.out.println(l);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
