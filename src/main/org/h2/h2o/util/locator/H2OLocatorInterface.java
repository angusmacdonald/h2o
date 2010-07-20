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
package org.h2.h2o.util.locator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.h2.h2o.remote.StartupException;
import org.h2.h2o.util.locator.messages.ReplicaLocationsResponse;

/**
 * Used to find an existing database system by connecting through the set of known nodes (system table nodes).
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class H2OLocatorInterface {

	private static final int MINIMUM_NUMER_OF_LOCATOR_SERVERS = 1;
	private String[] locatorLocations;

	/**
	 * Connections to locator servers and the corresponding last known update count on that server.
	 */
	private Map<LocatorClientConnection, Integer> locatorConnections;
	private DatabaseDescriptorFile descriptor;



	/**
	 * Finds locator server locations from the descriptor file.
	 * @param databaseName		Name of the database being connected to.
	 * @param descriptorURL		Location of the descriptor file.
	 * @throws IOException
	 * @throws StartupException 
	 */
	public H2OLocatorInterface(String databaseName, String descriptorURL) throws IOException, StartupException{
		descriptor = new DatabaseDescriptorFile(descriptorURL);
		this.locatorLocations = descriptor.getLocatorLocations();
		connectToLocators();
	}

	/**
	 * Used for testing where the locator locations are already known.
	 */
	public H2OLocatorInterface(String[] locatorLocations) throws IOException{
		this.locatorLocations = locatorLocations;
		connectToLocators();
	}

	/**
	 * Connect to all of the specified locator servers.
	 */
	private void connectToLocators() throws IOException {
		locatorConnections = new HashMap<LocatorClientConnection, Integer>();

		for (String location: locatorLocations){
			try {
				LocatorClientConnection lcc = getLocatorConnection(location);

				if (lcc.checkIsConnected()){
					locatorConnections.put(lcc, 0);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (locatorConnections.size() == 0){
			throw new IOException("Could not connect to any locator servers.");
		}
	}

	/**
	 * Get the set of database locations which hold valid, up-to-date, system table state.
	 * @return	Database URLs represented as strings, where system table state is stored.
	 * @throws IOException	Thrown if the method was unable to connect to a locator server.
	 */
	public List<String> getLocations() throws IOException{

		ReplicaLocationsResponse majorityResponse = null;
		List<ReplicaLocationsResponse> resultList = new LinkedList<ReplicaLocationsResponse>();
		Set<ReplicaLocationsResponse> resultSet = new HashSet<ReplicaLocationsResponse>();

		int initialMatches = 1;
		boolean foundMajority = false;

		/*
		 * Get responses. Loop through each locator server.
		 */
		for (Entry<LocatorClientConnection, Integer> locatorLocation: locatorConnections.entrySet()){

			LocatorClientConnection lcc = locatorLocation.getKey();

			if (!lcc.checkIsConnected()){ //not connected.
				break;
			}
			ReplicaLocationsResponse response = lcc.getDatabaseLocations();

			locatorLocation.setValue(response.getUpdateCount()); //update updatecount

			if (!foundMajority){ // if we've found the majority the only thing needing updated is the update count.
				resultList.add(response);
				boolean newEntry = resultSet.add(response);

				if (!newEntry || locatorConnections.size() == 1) { //if we quickly get a majority, use this.
					initialMatches++;
					if (initialMatches >= (locatorConnections.size()/2) + 1) {
						majorityResponse = response;
						foundMajority = true;
					}
					//XXX this doesn't record the database locations returned by each locator server. Does this matter?
				}
			}
		}


		if (!foundMajority){
			/*
			 * Pick majority response.
			 */
			Map<ReplicaLocationsResponse, Integer> matches = new HashMap<ReplicaLocationsResponse, Integer>();

			for (ReplicaLocationsResponse response: resultList){
				Integer count = matches.get(response);

				if (count == null){
					matches.put(response, 1);
				} else {
					matches.put(response, (count+1));
				}
			}

			int currentMax = 0;
			for (Entry<ReplicaLocationsResponse, Integer> entry: matches.entrySet()){
				int count = entry.getValue();

				if (count > currentMax){
					majorityResponse = entry.getKey();
					currentMax = count;
				}
			}

			if (currentMax < (resultList.size()/2)+1) return null; // no majority found.
		}


		return majorityResponse.getLocations();
	}

	/**
	 * Update the set of valid system table replica locations on the locator files.
	 * @param replicaLocations	Database URLs represented as strings, where system table state is stored.
	 * @throws IOException Thrown if the method was unable to connect to a locator server.
	 */
	public boolean setLocations(String[] replicaLocations) throws IOException {

		//boolean[] successful = new boolean[locatorLocations.length];
		int successCount = 0;
		boolean successful = false;

		for (String locatorLocation: locatorLocations){

			LocatorClientConnection lcc = getLocatorConnection(locatorLocation);

			if (!lcc.checkIsConnected()){
				successful = false;
			} else {
				successful = lcc.sendDatabaseLocation(replicaLocations);
			}

			if (successful) successCount ++;
		}

		return hasAchievedMajority(successCount, locatorConnections.size());
	}

	public boolean lockLocators(String databaseInstanceString) throws IOException, StartupException {
		int successful = 0;

		if (locatorLocations.length < MINIMUM_NUMER_OF_LOCATOR_SERVERS){
			throw new StartupException("Not enough locator servers to reach majority consensus.");
		}

		for (Entry<LocatorClientConnection, Integer> locatorLocation: locatorConnections.entrySet()){
			LocatorClientConnection lcc = locatorLocation.getKey();

			if (!lcc.checkIsConnected()){
				break;
			}
			int result = lcc.requestLock(databaseInstanceString);

			if (locatorLocation.getValue() == 0){
				throw new StartupException("Update count for a locator server is still set at 0. A get message should have" +
				" been sent by this client before a lock request.");
			} else if (result > 0 && locatorLocation.getValue().equals(result)){
				//If the lock was taken out (result is greater than zero) and the update count hasn't changed.
				successful++;
			}
		}

		return hasAchievedMajority(successful, locatorConnections.size());
	}



	public boolean commitLocators(String databaseInstanceString) throws IOException, StartupException {
		int successful = 0;

		if (locatorConnections.size() < MINIMUM_NUMER_OF_LOCATOR_SERVERS){
			throw new StartupException("Not enough locator servers to reach majority consensus.");
		}

		for (Entry<LocatorClientConnection, Integer> locatorLocation: locatorConnections.entrySet()){
			LocatorClientConnection lcc = locatorLocation.getKey();
			boolean unlocked = lcc.confirmSystemTableCreation(databaseInstanceString);
			if (unlocked) successful++;
		}

		return hasAchievedMajority(successful, locatorConnections.size());
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
			e.printStackTrace();
			throw new IOException("Failed to parse locator location from database descriptor. Ensure the descriptor file lists locators as host:port combinations.");
		}
		LocatorClientConnection lcc = new LocatorClientConnection(host, port);
		return lcc;

	}

	/**
	 * Whether a majority has been achieved. Whether successfulResponses is greater than or equal to half
	 * of numberOfLocators + 1.
	 * 
	 * This is public to allow junit tests to access it.
	 */
	public static boolean hasAchievedMajority(int successfulResponses, int numberOfLocators) {
		return successfulResponses >= (numberOfLocators/2 + 1);
	}

	public DatabaseDescriptorFile getDescriptor() {
		return descriptor;
	}


}
