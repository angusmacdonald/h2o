/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * Copyright 2009-2010 University of St Andrews. Licensed under the Eclipse Public License, Version 1.0
 * http://www.eclipse.org/legal/epl-v10.html Initial Developer: University of St Andrews
 */

package org.h2o.autonomic.settings;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.h2o.db.id.DatabaseID;
import org.h2o.locator.DatabaseDescriptor;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * There are three levels of settings. Local settings (specified in the database instances properties file) take precedence. If any settings
 * are not specified here the settings in the database descriptor file are used. If there are still some settings left unspecified, a
 * standard configuration is used.
 * 
 * When the database is started this configuration is stored in the local settings file.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class Settings {

    /**
     * The set of table names that don't represent user tables, but system commands or specialized operations.
     */
    public static final Set<String> reservedTableNames = new HashSet<String>();

    public static final String JDBC_PORT = "JDBC_PORT";

    /**
     * Whether to monitor the types of queries being made to the Table Manager, and the location these queries are
     * coming from.
     */
    public static final boolean QUERY_MONITORING_ENABLED = true; //TODO make into setting in properties file.

    /**
     * The maximum number of samples that a Table Manager may take while query monitoring before monitoring data must be trimmed.
     */
    public static final int MAX_NUMBER_OF_TABLE_MANAGER_SAMPLES = 1000;

    static {
        reservedTableNames.add("SYSTEM_RANGE");
        reservedTableNames.add("TABLE");
    }

    private final H2OPropertiesWrapper localSettings;
    private final DatabaseDescriptor globalSettings;

    public Settings(final H2OPropertiesWrapper localSettings, final DatabaseDescriptor databaseDescriptorFile) throws StartupException {

        this.localSettings = localSettings;
        globalSettings = databaseDescriptorFile;

        /*
         * 1. Load Local Settings.
         */
        try {
            localSettings.loadProperties();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        /*
         * 2. Iterate through global settings. If there is a setting here which is not specified by the local settings, add it to the local
         * settings.
         */
        iterateThroughDatabaseSettings(globalSettings.getSettings());

        /*
         * 3. Iterate through default settings. If there is a setting here which is not specified by the local and global settings, add it
         * to the local settings.
         */
        iterateThroughDatabaseSettings(defaultSettings());

    }

    /**
     * Iterates through database settings in the specified properties file looking for anything that has not already been specified at a
     * lower level.
     */
    public void iterateThroughDatabaseSettings(final Properties settings) {

        for (final Entry<Object, Object> entry : settings.entrySet()) {
            final String propertyName = (String) entry.getKey();
            final String propertyValue = (String) entry.getValue();

            if (localSettings.getProperty(propertyName) == null) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Updating setting " + propertyName + " = " + propertyValue);
                localSettings.setProperty(propertyName, propertyValue);
            }
        }
    }

    public static Properties defaultSettings() {

        final Properties defaults = new Properties();

        // The time in-between attempts to replicate the system table's state
        // onto a new successor.
        defaults.setProperty("REPLICATOR_SLEEP_TIME", "2000");

        // The number of copies required for individual relations in the system.
        defaults.setProperty("RELATION_REPLICATION_FACTOR", "2");

        // Number of copies required of the System Table's state.
        defaults.setProperty("SYSTEM_TABLE_REPLICATION_FACTOR", "2");

        // Number of copies required of Table Manager state.
        defaults.setProperty("TABLE_MANAGER_REPLICATION_FACTOR", "2");

        /*
         * The number of times a database should attempt to connect to an active instance when previous attempts have failed due to bind
         * exceptions. <p>For example, if the value of this is 100, the instance will attempt to create a server on 100 different ports
         * before giving up.
         */
        defaults.setProperty("ATTEMPTS_AFTER_BIND_EXCEPTIONS", "100");

        /*
         * The number of attempts that an instance will make to create/join a database system. It may fail because a lock is held on
         * creating the System Table, or because no System Table instances are active.
         */
        defaults.setProperty("ATTEMPTS_TO_CREATE_OR_JOIN_SYSTEM", "20");

        /*
         * Whether the system is replicating the meta-data of the System Table and Table Managers.
         */
        defaults.setProperty("METADATA_REPLICATION_ENABLED", "true");

        /*
         * The time between checks to ensure that database meta-data is sufficiently replicated.
         */
        defaults.setProperty("METADATA_REPLICATION_THREAD_SLEEP_TIME", "5000");

        /*
         * The time between checks to ensure that Table Managers are still running.
         */
        defaults.setProperty("TABLE_MANAGER_LIVENESS_CHECKER_THREAD_SLEEP_TIME", "10000");

        /*
         * Whether diagnostic events are to be consumed and sent to an event monitor.
         */
        defaults.setProperty("DATABASE_EVENTS_ENABLED", "false");

        /*
         * The location of the event server that is to receive diagnostic events. This field doesn't matter if DATABASE_EVENTS_ENABLED
         * is set to false.
         */
        defaults.setProperty("EVENT_SERVER_LOCATION", "localhost");

        /*
         * The number of replicas that must update before an update can take place.
         */
        defaults.setProperty("ASYNCHRONOUS_REPLICATION_FACTOR", "2");

        /*
         * Whether asynchronous replication is enabled.
         */
        defaults.setProperty("ASYNCHRONOUS_REPLICATION_ENABLED", "true");

        /*
         * If true this delays the commit of an insert query. This should always be false unless you are testing asynchronous updates.
         */
        defaults.setProperty("DELAY_QUERY_COMMIT", "false");

        /*
         * Sets the default level of diagnostics for the database system.
         */
        defaults.setProperty("diagnosticLevel", "INIT"); //Options that make sense for this system: NONE, FINAL, INIT, FULL

        /*
         * The preferred port on which the table manager server will run. 
         */
        defaults.setProperty("TABLE_MANAGER_SERVER_PORT", "60100");

        /*
         * The preferred port on which the system table server will run. 
         */
        defaults.setProperty("SYSTEM_TABLE_SERVER_PORT", "61100");

        /*
         * The preferred port on which the database instance server will run. 
         */
        defaults.setProperty("DATABASE_INSTANCE_SERVER_PORT", "62100");

        /*
         * The location of the numonic configuration file, which specifies what the database's numonic instance will monitor.
         */
        defaults.setProperty("NUMONIC_MONITORING_FILE_LOCATION", "default_numonic_settings.properties");

        /*
         * The location of the numonic thresholds file, which specifies what thresholds the database will monitor (and alert observer components
         * when they are exceeded).
         */
        defaults.setProperty("NUMONIC_THRESHOLDS_FILE_LOCATION", "default_numonic_thresholds.properties");

        /*
         * Whether numonic should be started with this database instance.
         */
        defaults.setProperty("NUMONIC_MONITORING_ENABLED", "true");

        return defaults;
    }

    public H2OPropertiesWrapper getLocalSettings() {

        return localSettings;
    }

    public String get(final String string) {

        return localSettings.getProperty(string);
    }

    public void set(final String key, final String value) {

        localSettings.setProperty(key, value);
    }

    public static void saveAsLocalProperties(final Properties newSettings, final String databaseName) throws IOException {

        /*
         * Load any existing properties file because there might be other info that we don't want to delete. For example, the location of
         * any locators.
         */
        final H2OPropertiesWrapper localSettings = H2OPropertiesWrapper.getWrapper(DatabaseID.parseURL(databaseName));
        try {
            localSettings.loadProperties();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        // Overwrite local database settings with those provided via newSettings
        // parameter.
        for (final Entry<Object, Object> entry : newSettings.entrySet()) {
            final String propertyName = (String) entry.getKey();
            final String propertyValue = (String) entry.getValue();

            localSettings.setProperty(propertyName, propertyValue);
        }

        localSettings.saveAndClose();
    }
}
