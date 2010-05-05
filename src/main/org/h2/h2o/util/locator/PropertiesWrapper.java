package org.h2.h2o.util.locator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class PropertiesWrapper {

	protected Properties properties;
	protected String propertiesFileLocation;
	protected FileOutputStream fos;


	protected static final String DATABASENAME = "databaseName";
	protected static final String CREATIONDATE = "creationDate";
	
	/**
	 * 
	 */
	public PropertiesWrapper() {
		super();
	}


	public void createPropertiesFile(){
		/*
		 * Create the properties file.
		 */
		File f = new File(propertiesFileLocation);
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			fos = new FileOutputStream(propertiesFileLocation);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}


	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}