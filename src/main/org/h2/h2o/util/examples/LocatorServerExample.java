package org.h2.h2o.util.examples;
import org.h2.h2o.deployment.H2OLocator;

/**
 * Starts a new locator server instance and creates a corresponding H2O descriptor file for this database domain.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LocatorServerExample {


	public static void main(String[] args) {
		
		String databaseName = "MyFirstDatabase";//the name of the database domain.
		int tcpPort = 9998;						//the port on which the locator server will run.
		boolean createDescriptor = true;		//whether a database descriptor file will be created.
		String rootFolder = "db_data"; 			//where the locator file (and descriptor file) will be created.
		boolean startInSeperateThread = false; 	//whether the locator server should be started in a seperate thread (it blocks).
		
		H2OLocator locator = new H2OLocator(databaseName, tcpPort, createDescriptor, rootFolder);
		
		locator.start(startInSeperateThread);
		
	}

}
