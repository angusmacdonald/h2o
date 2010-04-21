package org.h2.h2o.util.properties;

import org.h2.h2o.remote.ChordRemote;
import org.h2.h2o.util.DatabaseURL;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class StaticServerSetup {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		setUpStaticDescriptorFiles();
	}

	/**
	 * 
	 */
	public static void setUpStaticDescriptorFiles() {
		String databaseName = "angusDB";
		
		String descriptorFile = "http://www.cs.st-andrews.ac.uk/~angus/databases/" + databaseName + ".h2o";
		
		String initialSchemaManager = "jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db";
		
		
		/*
		 * Setup descriptor file.
		 */
		DatabaseDescriptorFile ddf = new DatabaseDescriptorFile("\\\\shell\\angus\\public_html\\databases\\" + databaseName + ".h2o");
		ddf.createPropertiesFile();
		ddf.setProperties(databaseName, "\\\\shell\\angus\\public_html\\databases");
		//System.out.println("\\\\shell\\angus\\public_html\\databases" + databaseName + ".h2o");
		/*
		 * Setup locator file.
		 */
//		DatabaseLocatorFile dlf = new DatabaseLocatorFile("angusDB", "\\\\shell\\angus\\public_html\\databases"); 
//		dlf.setProperties("testDB", initialSchemaManager + "+" + ChordRemote.currentPort);
//		
		/*
		 * Setup bootstrap files.
		 */
		H2oProperties knownHosts = new H2oProperties(DatabaseURL.parseURL(initialSchemaManager));
		knownHosts.createNewFile();
		knownHosts.setProperty("descriptor", descriptorFile);
		knownHosts.setProperty("databaseName", databaseName);
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:tcp://localhost:9191/db_data/three/test_db"));
		knownHosts.createNewFile();
		knownHosts.setProperty("descriptor", descriptorFile);
		knownHosts.setProperty("databaseName", databaseName);
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:tcp://localhost:9292/db_data/two/test_db"));
		knownHosts.createNewFile();
		knownHosts.setProperty("descriptor", descriptorFile);
		knownHosts.setProperty("databaseName", databaseName);
		knownHosts.saveAndClose();
	}

}
