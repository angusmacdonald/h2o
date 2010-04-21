package org.h2.h2o.util.properties;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseLocatorFile extends PropertiesWrapper {
	private static final String DATABASELOCATIONS = "databaseLocations";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileLocation = "\\\\shell\\angus\\public_html\\databases";
		
		//DatabaseLocatorFile cddf = new DatabaseLocatorFile("testDB", fileLocation);
		//cddf.createPropertiesFile();
		//cddf.setProperties("testDB", "jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db", "jdbc:h2:tcp://localhost:9292/db_data/two/test_db");
	}
	
//	public DatabaseLocatorFile(String databaseName, String locatorFileFolder){
//
//		this.propertiesFileLocation = locatorFileFolder + "\\" + databaseName + "Locator.h2ol";
//		this.properties = new Properties();
//		
//
//	}
	
	public String[] getDatabaseLocations(){
		try {

			FileInputStream fis = new FileInputStream(propertiesFileLocation);
			properties.load(fis);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String databaseLocations = properties.getProperty(DATABASELOCATIONS);
		return databaseLocations.split(",");
	}
	
	public void setProperties(String databaseName, String... databaseLocations){

		String locatorLocations = "";
		
		for (String locatorFile: databaseLocations){
			locatorLocations += locatorFile + ",";
		}
		locatorLocations = locatorLocations.substring(0, locatorLocations.length()-1);
		
		properties.setProperty(DATABASENAME, databaseName);
		properties.setProperty(CREATIONDATE, new Date().getTime() + "");
		properties.setProperty(DATABASELOCATIONS, locatorLocations);
		
		
		try {
			fos = new FileOutputStream(propertiesFileLocation);
			
			properties.store(fos, "H2O Database Locator File.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
