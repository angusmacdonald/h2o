package org.h2.h2o.util;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocalH2OProperties {

	private Properties properties;

	private String propertiesFileLocation;

	private FileOutputStream fos = null;

	private FileInputStream fis = null;


	@Deprecated
	public LocalH2OProperties(){
		this.properties = new Properties();
		//Required because this is a test class.
	}

	/**
	 * @param dbURL	The URL of this database instance. This is used to name and locate
	 * the properties file for this database on disk.
	 * @param appendum A string to be added on to the DBurl as part of the properties file name.
	 */
	public LocalH2OProperties(DatabaseURL dbURL) {
		this.properties = new Properties();
		this.propertiesFileLocation = "config" + File.separator + dbURL.getDbLocationWithoutIllegalCharacters() + ".properties";
	}

	/**
	 * @param string
	 */
	public LocalH2OProperties(String descriptorLocation) {
		this.properties = new Properties();
		this.propertiesFileLocation = "config" + File.separator + descriptorLocation + ".properties";
	}

	public void setPropertiesFileLocation(DatabaseURL dbURL) {
		this.propertiesFileLocation = dbURL.getDbLocationWithoutIllegalCharacters() + ".properties";
	}

	public boolean loadProperties(){
		File f = new File(propertiesFileLocation);
		if (!f.exists()) return false;
		//This check is necessary because a file will be created when FileInputStream is created.

		if (fis == null){
			try {

				this.fis = new FileInputStream(propertiesFileLocation);
			} catch (FileNotFoundException e) {
				//won't happen.
			}
		}


		try{
			this.properties.load(fis);
		} catch (Exception e){
			return false;
		}
		return true;
	}

	/**
	 * Deletes any existing properties file with the given name and creates a new one.
	 */
	public void createNewFile() {
		removePropertiesFile();

		File f = new File("config");
		if (!f.exists()){
			f.mkdir();
		}
		
		f = new File(propertiesFileLocation);
		try {
			f.createNewFile();
		} catch (IOException e) {
			ErrorHandling.exceptionError(e, "Creation of properties file failed at " + propertiesFileLocation + ".");
			e.printStackTrace();
		}

		try {
			this.fis = new FileInputStream(propertiesFileLocation);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	public String getProperty(String key) {

		String value = null;

		if (properties.getProperty(key) != null) {
			value = properties.getProperty(key);
		}

		return value;
	}

	public void setProperty(String key, String value) {

		properties.setProperty(key, value);
		
	}

	private boolean removePropertiesFile(){
		try {
			if (fis != null) fis.close(); fis = null;
			if (fos != null) fos.close(); fos = null;
		} catch (IOException e) {
			e.printStackTrace();
		}

		File f = new File(propertiesFileLocation);

		return f.delete();
	}


	/**
	 * 
	 */
	public void saveAndClose() {
		
		try {

			if (fos == null){

				this.fos = new FileOutputStream(propertiesFileLocation);
			}

			properties.store(this.fos, "Properties for a single database instance.");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		try {
			if (fos != null) fos.close();

			if (fis != null) fis.close();

		} catch (IOException e) {
		}
	}

	/*
	 * ####################################################
	 * 
	 * TESTING
	 * 
	 * ####################################################
	 */
	@Test
	public void testBasicProperties(){
		DatabaseURL dbURL = DatabaseURL.parseURL("jdbc:h2:mem:two");
		LocalH2OProperties testProp = new LocalH2OProperties(dbURL);

		String key = "systemTableLocation";
		String value = "jdbc:h2:sm:mem:one";
		testProp.setProperty(key, value);

		assertEquals(value, testProp.getProperty(key));

		assert testProp.removePropertiesFile();
	}

	@Test
	public void testMultipleProperties(){
		DatabaseURL dbURL = DatabaseURL.parseURL("jdbc:h2:mem:two");
		LocalH2OProperties testProp = new LocalH2OProperties(dbURL);

		testProperties(testProp);

		assert testProp.removePropertiesFile();
	}

	@Test
	public void testAlternateConstructor(){
		DatabaseURL dbURL = DatabaseURL.parseURL("jdbc:h2:mem:two");
		LocalH2OProperties testProp = new LocalH2OProperties();
		testProp.setPropertiesFileLocation(dbURL);

		testProperties(testProp);

		assert testProp.removePropertiesFile();
	}

	@Test
	public void testAlternateConstructorFail(){
		LocalH2OProperties testProp = new LocalH2OProperties();

		try {
			testProperties(testProp);
			fail("Should throw an exception.");
		} catch (NullPointerException e){
			//expected.
		}
	}

	/**
	 * Tests loading the properties from file.
	 * 
	 * This isn't a great unit test because it relies on being run after @see {@link #multiplePropertiesTest()}
	 */
	@Test
	public void testLoad(){
		DatabaseURL dbURL = DatabaseURL.parseURL("jdbc:h2:mem:two");
		LocalH2OProperties testProp = new LocalH2OProperties(dbURL);

		testProperties(testProp);

		assert testProp.removePropertiesFile();

		testProp.loadProperties();

		String key = "systemTableLocation";
		String value = "jdbc:h2:sm:mem:one";

		String key2 = "localMachineLocation";
		String value2 = "jdbc:h2:mem:two";

		assertEquals(value, testProp.getProperty(key));
		assertEquals(value2, testProp.getProperty(key2));
	}

	/**
	 * Tries to load a properties file that doesn't exist. Should return false.
	 */
	@Test
	public void testLoadPropertiesFail(){
		DatabaseURL dbURL = DatabaseURL.parseURL("jdbc:h2:mem:two");
		LocalH2OProperties testProp = new LocalH2OProperties(dbURL);

		assertFalse(testProp.loadProperties());

	}

	/**
	 * @param testProp
	 */
	private void testProperties(LocalH2OProperties testProp) {
		String key = "systemTableLocation";
		String value = "jdbc:h2:sm:mem:one";
		testProp.setProperty(key, value);

		String key2 = "localMachineLocation";
		String value2 = "jdbc:h2:mem:two";
		testProp.setProperty(key2, value2);

		assertEquals(value, testProp.getProperty(key));
		assertEquals(value2, testProp.getProperty(key2));
	}

	/**
	 * Returns a set of the keys in this properties file.
	 * @return
	 */
	public Set<Object> getKeys() {
		return properties.keySet();
	}



}