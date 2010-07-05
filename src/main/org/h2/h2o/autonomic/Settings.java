/*
 * Copyright 2009-2010 University of St Andrews. Licensed under the Eclipse Public License, Version 1.0
 * http://www.eclipse.org/legal/epl-v10.html
 * Initial Developer: University of St Andrews
 */

package org.h2.h2o.autonomic;

import java.util.Map.Entry;
import java.util.Properties;

import org.h2.h2o.remote.StartupException;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.LocalH2OProperties;
import org.h2.h2o.util.locator.DatabaseDescriptorFile;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * There are three levels of settings. Local settings (specified in the database instances properties file) take
 * precedence. If any settings are not specified here the settings in the database descriptor file are used. If there
 * are still some settings left unspecified, a standard configuration is used.
 * 
 * When the database is started this configuration is stored in the local settings file.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class Settings {

	private LocalH2OProperties localSettings;

	private DatabaseDescriptorFile globalSettings;

	public Settings(LocalH2OProperties localSettings, DatabaseDescriptorFile databaseDescriptorFile) throws StartupException {
		this.localSettings = localSettings;
		this.globalSettings = databaseDescriptorFile;

		/*
		 * 1. Load Local Settings.
		 */
		localSettings.loadProperties();

		/*
		 * 2. Iterate through global settings. If there is a setting here which is not specified
		 * by the local settings, add it to the local settings.
		 */
		iterateThroughDatabaseSettings(globalSettings.getSettings());

		/*
		 * 3. Iterate through default settings. If there is a setting here which is not specified
		 * by the local and global settings, add it to the local settings.
		 */
		iterateThroughDatabaseSettings(defaultSettings());

	}


	/**
	 * Iterates through database settings in the specified properties file looking for anything that has not already been specified at
	 * a lower level.
	 */
	public void iterateThroughDatabaseSettings(Properties settings) {
		for (Entry<Object, Object> entry: settings.entrySet()){
			String propertyName = (String) entry.getKey();
			String propertyValue = (String) entry.getValue();

			if (localSettings.getProperty(propertyName) == null){
				Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Updating setting " + propertyName + " = " + propertyValue);	
				localSettings.setProperty(propertyName, propertyValue);
			}
		}
	}

	public static Properties defaultSettings() {
		Properties defaults = new Properties();

		// The time in-between attempts to replicate the system table's state onto a new successor.
		defaults.setProperty("REPLICATOR_SLEEP_TIME", "2000");

		// The number of copies required for individual relations in the system.
		defaults.setProperty("RELATION_REPLICATION_FACTOR", "1");

		//Number of copies required of the System Table's state.
		defaults.setProperty("SYSTEM_TABLE_REPLICATION_FACTOR", "2");

		//Number of copies required of Table Manager state.
		defaults.setProperty("TABLE_MANAGER_REPLICATION_FACTOR", "2");

		/*
		 * The number of times a database should attempt to connect to an active instance
		 * when previous attempts have failed due to bind exceptions. 
		 * 
		 * <p>For example, if the value of this is 100, the instance will attempt to
		 * create a server on 100 different ports before giving up.
		 */
		defaults.setProperty("ATTEMPTS_AFTER_BIND_EXCEPTIONS", "100");

		/*
		 * The number of attempts that an instance will make to create/join a
		 * database system. It may fail because a lock is held on creating the
		 * System Table, or because no System Table instances are active.
		 */
		defaults.setProperty("ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM", "20");

		/*
		 * Whether the system is replicating the meta-data of the System Table
		 * and Table Managers.
		 */
		defaults.setProperty("METADATA_REPLICATION_ENABLED", "true");

		return defaults;
	}

	public LocalH2OProperties getLocalSettings() {
		return localSettings;
	}


	public String get(String string) {
		return localSettings.getProperty(string);
	}


	public static void saveAsLocalProperties(Properties newSettings, String databaseName) {

		/*
		 * Load any existing properties file because there might be other info that we don't want to delete.
		 * For example, the location of any locators. 
		 */
		LocalH2OProperties localSettings = new LocalH2OProperties(DatabaseURL.parseURL(databaseName));
		localSettings.loadProperties();

		//Overwrite local database settings with those provided via newSettings parameter.
		for (Entry<Object, Object> entry: newSettings.entrySet()){
			String propertyName = (String) entry.getKey();
			String propertyValue = (String) entry.getValue();

			localSettings.setProperty(propertyName, propertyValue);
		}
		
		localSettings.saveAndClose();
	}
}
