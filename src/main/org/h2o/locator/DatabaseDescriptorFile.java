/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import org.h2o.util.exceptions.StartupException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseDescriptorFile {
	
	private Properties properties;
	
	private String propertiesFileLocation;
	
	private FileOutputStream fos;
	
	private static final String DATABASENAME = "databaseName";
	
	private static final String CREATIONDATE = "creationDate";
	
	private static final String LOCATORLOCATIONS = "locatorLocations";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileLocation = "\\\\shell\\angus\\public_html\\databases";
		
		DatabaseDescriptorFile cddf = new DatabaseDescriptorFile("testDB", fileLocation);
		cddf.createPropertiesFile();
		cddf.setLocatorLocations("testDB", fileLocation);
	}
	
	public DatabaseDescriptorFile(String databaseName, String propertiesFileFolder) {
		
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
	
	public String[] getLocatorLocations() throws StartupException {
		
		openPropertiesFile();
		
		String locatorLocations = properties.getProperty(LOCATORLOCATIONS);
		
		return locatorLocations.split(",");
	}
	
	private void openPropertiesFile() throws StartupException {
		if ( propertiesFileLocation.startsWith("http:") ) { // Parse URL, request
															// file from
															// webpage.
			
			try {
				URL url = new URL(propertiesFileLocation);
				InputStreamReader isr = new InputStreamReader(url.openStream());
				
				properties.load(isr);
				
			} catch ( IOException e ) {
				e.printStackTrace();
			}
		} else { // Try to open the file from disk.
			File f = new File(propertiesFileLocation);
			FileInputStream fis;
			try {
				fis = new FileInputStream(f);
				properties.load(fis);
			} catch ( FileNotFoundException e ) {
				e.printStackTrace();
				throw new StartupException(e.getMessage());
			} catch ( IOException e ) {
				e.printStackTrace();
				throw new StartupException(e.getMessage());
			}
			
		}
	}
	
	public void setLocatorLocations(String databaseName, String... locations) {
		String locatorLocations = "";
		
		for ( String locatorFile : locations ) {
			locatorLocations += locatorFile + ",";
		}
		locatorLocations = locatorLocations.substring(0, locatorLocations.length() - 1);
		
		properties.setProperty(DATABASENAME, databaseName);
		properties.setProperty(CREATIONDATE, new Date().getTime() + "");
		properties.setProperty(LOCATORLOCATIONS, locatorLocations);
		
		try {
			// fos = new FileOutputStream(propertiesFileLocation);
			properties.store(fos, "H2O Database Descriptor File.");
		} catch ( IOException e ) {
			e.printStackTrace();
		}
	}
	
	public Properties getSettings() throws StartupException {
		
		openPropertiesFile();
		
		return properties;
	}
	
	public void createPropertiesFile() {
		/*
		 * Create the properties file.
		 */
		File f = new File(propertiesFileLocation);
		try {
			f.createNewFile();
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		
		try {
			fos = new FileOutputStream(propertiesFileLocation);
		} catch ( FileNotFoundException e ) {
			e.printStackTrace();
		}
	}
	
	public String getProperty(String key) {
		return properties.getProperty(key);
	}
}
