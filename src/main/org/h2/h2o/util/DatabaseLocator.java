package org.h2.h2o.util;

import java.io.IOException;
import java.util.Set;

import org.h2.h2o.comms.ReplicaManager;
import org.h2.h2o.util.properties.DatabaseDescriptorFile;
import org.h2.h2o.util.properties.DatabaseLocatorFile;
import org.h2.h2o.util.properties.server.LocatorClientConnection;

/**
 * Used to find an existing database system by connecting through the set of known nodes (schema manager nodes).
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseLocator {

	private Set<String> databaseLocations = null;
	private String[] locatorLocations;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String databaseName = "testDB";
		String descriptorURL = "http://www.cs.st-andrews.ac.uk/~angus/databases";

		DatabaseLocator dl = new DatabaseLocator(databaseName, descriptorURL);

		Set<String> locations;
		try {
			locations = dl.getLocations();


			for (String l: locations){
				System.out.println(l);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public DatabaseLocator(String databaseName, String descriptorURL){
		DatabaseDescriptorFile descriptor = new DatabaseDescriptorFile(descriptorURL);
		locatorLocations = descriptor.getLocatorLocations();
	}

	public Set<String> getLocations() throws IOException{
		for (String locatorLocation: locatorLocations){

			LocatorClientConnection lcc = new LocatorClientConnection("eigg", 29999);

			Set<String> databaseLocations = lcc.getDatabaseLocations();


			if (databaseLocations != null && databaseLocations.size() > 0){
				return databaseLocations;
			}
		}

		return null;
	}

	/**
	 * @param stateReplicaManager
	 * @return
	 * @throws IOException 
	 */
	public void setLocations(String[] replicaLocations) throws IOException {
		for (String locatorLocation: locatorLocations){

			LocatorClientConnection lcc = new LocatorClientConnection("eigg", 29999);

			lcc.sendDatabaseLocation(replicaLocations);
		}


	}
}
