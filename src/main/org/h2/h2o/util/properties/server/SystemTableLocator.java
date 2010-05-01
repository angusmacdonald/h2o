package org.h2.h2o.util.properties.server;

import java.io.IOException;
import java.util.Set;

import org.h2.h2o.remote.StartupException;
import org.h2.h2o.util.properties.DatabaseDescriptorFile;

/**
 * Used to find an existing database system by connecting through the set of known nodes (system table nodes).
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableLocator {

	private static final int MINIMUM_NUMER_OF_LOCATOR_SERVERS = 0;
	private String[] locatorLocations;

	public SystemTableLocator(String databaseName, String descriptorURL){
		DatabaseDescriptorFile descriptor = new DatabaseDescriptorFile(descriptorURL);
		locatorLocations = descriptor.getLocatorLocations();
	}

	/**
	 * Get the set of database locations which hold valid, up-to-date, system table state.
	 * @return	Database URLs represented as strings, where system table state is stored.
	 * @throws IOException	Thrown if the method was unable to connect to a locator server.
	 */
	public Set<String> getLocations() throws IOException{
		for (String locatorLocation: locatorLocations){

			LocatorClientConnection lcc = getLocatorConnection(locatorLocation);

			Set<String> databaseLocations = lcc.getDatabaseLocations();


			if (databaseLocations != null && databaseLocations.size() > 0){
				//Return the first set of locations found.
				return databaseLocations;
			}
		}

		return null;
	}

	/**
	 * Update the set of valid system table replica locations on the locator files.
	 * @param replicaLocations	Database URLs represented as strings, where system table state is stored.
	 * @throws IOException Thrown if the method was unable to connect to a locator server.
	 */
	public void setLocations(String[] replicaLocations) throws IOException {
		for (String locatorLocation: locatorLocations){

			LocatorClientConnection lcc = getLocatorConnection(locatorLocation);

			lcc.sendDatabaseLocation(replicaLocations);
		}
	}

	public boolean lockLocators(String databaseInstanceString) throws IOException, StartupException {
		int successful = 0;
		
		if (locatorLocations.length < MINIMUM_NUMER_OF_LOCATOR_SERVERS){
			throw new StartupException("Not enough locator servers to reach majority consensus.");
		}
		
		for (String locatorLocation: locatorLocations){
			LocatorClientConnection lcc = getLocatorConnection(locatorLocation);
			boolean locked = lcc.lockLocator(databaseInstanceString);
			
			if (locked) successful++;
		}
		
		return successful == locatorLocations.length;
		//TODO use this when more locators are used - return successful > (locatorLocations.length / 2 + 1);
	}
	
	public boolean unlockLocators(String databaseInstanceString) throws IOException, StartupException {
		int successful = 0;
		
		if (locatorLocations.length < MINIMUM_NUMER_OF_LOCATOR_SERVERS){
			throw new StartupException("Not enough locator servers to reach majority consensus.");
		}
		
		for (String locatorLocation: locatorLocations){
			LocatorClientConnection lcc = getLocatorConnection(locatorLocation);
			boolean unlocked = lcc.unlockLocator(databaseInstanceString);
			
			if (unlocked) successful++;
		}
		
		return successful == locatorLocations.length;
		//TODO use this when more locators are used - return successful > (locatorLocations.length / 2 + 1);
	}


	/**
	 * Obtain a new connection to the locator server.
	 * @param locatorLocation	String of the form 'host:port'.
	 * @return	New connection to the locator server.
	 */
	private LocatorClientConnection getLocatorConnection(String locatorLocation) throws IOException {
		
		String host = "";
		int port = 0;
		
		
		try{
			String[] locatorLocatonAddress = locatorLocation.split(":");
		
		host = locatorLocatonAddress[0];
		port = Integer.parseInt(locatorLocatonAddress[1]);
		

		} catch (Exception e){
			throw new IOException("Failed to parse locator location from database descriptor. Ensure the descriptor file lists locators as host:port combinations.");
		}
		LocatorClientConnection lcc = new LocatorClientConnection(host, port);
		return lcc;
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String databaseName = "testDB";
		String descriptorURL = "http://www.cs.st-andrews.ac.uk/~angus/databases";

		SystemTableLocator dl = new SystemTableLocator(databaseName, descriptorURL);

		Set<String> locations;
		try {
			locations = dl.getLocations();


			for (String l: locations){
				System.out.println(l);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
