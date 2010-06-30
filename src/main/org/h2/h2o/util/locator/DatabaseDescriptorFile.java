package org.h2.h2o.util.locator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import org.h2.h2o.remote.StartupException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseDescriptorFile extends PropertiesWrapper {


	private static final String LOCATORLOCATIONS = "locatorLocations";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileLocation = "\\\\shell\\angus\\public_html\\databases";

		DatabaseDescriptorFile cddf = new DatabaseDescriptorFile("testDB", fileLocation);
		cddf.createPropertiesFile();
		cddf.setProperties("testDB", fileLocation);
	}

	public DatabaseDescriptorFile(String databaseName, String propertiesFileFolder){

		this.propertiesFileLocation = propertiesFileFolder + "/" + databaseName + ".h2o";
		this.properties = new Properties();

	}

	/**
	 * @param url
	 */
	public DatabaseDescriptorFile(String url) {
		this.propertiesFileLocation = url;
		this.properties = new Properties();
	}

	public String[] getLocatorLocations() throws StartupException{

		if (propertiesFileLocation.startsWith("http:")){ //Parse URL, request file from webpage.

			try {
				URL url = new URL(propertiesFileLocation);
				InputStreamReader isr = new InputStreamReader(url.openStream());

				properties.load(isr);

			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {  //Try to open the file from disk.
			File f = new File(propertiesFileLocation);
			FileInputStream fis;
			try {
				fis = new FileInputStream(f);
				properties.load(fis);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				throw new StartupException(e.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				throw new StartupException(e.getMessage());
			}

		}

		String locatorLocations = properties.getProperty(LOCATORLOCATIONS);

		return locatorLocations.split(",");
	}

	public void setProperties(String databaseName, String... locations){
		String locatorLocations = "";

		for (String locatorFile: locations){
			locatorLocations += locatorFile + ",";
		}
		locatorLocations = locatorLocations.substring(0, locatorLocations.length()-1);

		properties.setProperty(DATABASENAME, databaseName);
		properties.setProperty(CREATIONDATE, new Date().getTime() + "");
		properties.setProperty(LOCATORLOCATIONS, locatorLocations);

		try {
			//fos = new FileOutputStream(propertiesFileLocation);
			properties.store(fos, "H2O Database Descriptor File.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
