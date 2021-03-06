/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

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

import org.h2o.db.manager.recovery.LocatorException;
import org.h2o.util.exceptions.StartupException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseDescriptor {

    private final Properties properties;

    private final String propertiesFileLocation;

    private FileOutputStream fos;

    private static final String DATABASENAME = "databaseName";

    private static final String CREATIONDATE = "creationDate";

    private static final String LOCATORLOCATIONS = "locatorLocations";

    public static void main(final String[] args) {

        final String fileLocation = "\\\\shell\\angus\\public_html\\databases";

        final DatabaseDescriptor cddf = new DatabaseDescriptor("testDB", fileLocation);
        cddf.createPropertiesFile();
        cddf.setLocatorLocations("testDB", fileLocation);
    }

    public DatabaseDescriptor(final String databaseName, final String propertiesFileFolder) {

        propertiesFileLocation = propertiesFileFolder + "/" + databaseName + ".h2o";
        properties = new Properties();
    }

    public DatabaseDescriptor(final String url) {

        propertiesFileLocation = url;
        properties = new Properties();
    }

    public String[] getLocatorLocations() throws LocatorException {

        try {
            openPropertiesFile();
        }
        catch (final StartupException e) {
            throw new LocatorException(e);
        }

        final String locatorLocations = properties.getProperty(LOCATORLOCATIONS);

        return locatorLocations.split(",");
    }

    private void openPropertiesFile() throws StartupException {

        if (propertiesFileLocation.startsWith("http:")) { // Parse URL, request file from webpage.
            InputStreamReader isr = null;
            try {
                final URL url = new URL(propertiesFileLocation);
                isr = new InputStreamReader(url.openStream());

                properties.load(isr);

            }
            catch (final IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    isr.close();
                }
                catch (final IOException e) {
                    //Doesn't matter at this point.
                }
            }
        }
        else { // Try to open the file from disk.
            final File file = new File(propertiesFileLocation);
            FileInputStream input_stream = null;
            try {
                input_stream = new FileInputStream(file);
                properties.load(input_stream);
            }
            catch (final FileNotFoundException e) {
                throw new StartupException(e.getMessage());
            }
            catch (final IOException e) {
                throw new StartupException(e.getMessage());
            }
            finally {
                try {
                    if (input_stream != null) {
                        input_stream.close();
                    }
                }
                catch (final IOException e) {
                    // Doesn't matter at this point.
                }
            }
        }
    }

    public void setLocatorLocations(final String databaseName, final String... locations) {

        final StringBuilder locatorLocations = new StringBuilder();
        for (final String locatorFile : locations) {

            locatorLocations.append(locatorFile);
            locatorLocations.append(",");

        }
        properties.setProperty(DATABASENAME, databaseName);
        properties.setProperty(CREATIONDATE, new Date().getTime() + "");
        properties.setProperty(LOCATORLOCATIONS, locatorLocations.substring(0, locatorLocations.length() - 1));

        try {
            // fos = new FileOutputStream(propertiesFileLocation);
            properties.store(fos, "H2O Database Descriptor File.");
        }
        catch (final IOException e) {
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
        final File f = new File(propertiesFileLocation);
        try {
            f.createNewFile();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        try {
            fos = new FileOutputStream(propertiesFileLocation);
        }
        catch (final FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(final String key) {

        return properties.getProperty(key);
    }
}
