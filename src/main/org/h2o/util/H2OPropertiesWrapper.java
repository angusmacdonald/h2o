/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010-2011 Distributed Systems Architecture Research Group *
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
package org.h2o.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.h2o.db.id.DatabaseID;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.util.IPropertiesWrapper;

/**
 * Wrapper for java properties class, which make it easier to create, load, and save properties.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2OPropertiesWrapper implements IPropertiesWrapper, Serializable {

    private static final long serialVersionUID = 836133291520954571L;

    private final Properties properties;
    private final String propertiesFileLocation;

    private String comment = "Properties File";

    private static final String TRUE = "true";

    private static Map<String, H2OPropertiesWrapper> map = new HashMap<String, H2OPropertiesWrapper>();

    private H2OPropertiesWrapper(final String fileLocation) {

        propertiesFileLocation = fileLocation;
        properties = new Properties();
    }

    /**
     * Gets a properties wrapper for a given file.
     *
     * @param fileLocation the location of the file containing properties
     * @return the wrapper
     */
    public static synchronized H2OPropertiesWrapper getWrapper(final String fileLocation) {

        H2OPropertiesWrapper wrapper = map.get(fileLocation);
        if (wrapper == null) {
            wrapper = new H2OPropertiesWrapper(fileLocation);
            map.put(fileLocation, wrapper);
        }
        return wrapper;
    }

    /**
     * Gets a properties wrapper for a given database.
     *
     * @param dbID the URL of this database instance. This is used to name and locate the properties file for this database on disk.
     * @return the wrapper
     */
    public static synchronized H2OPropertiesWrapper getWrapper(final DatabaseID dbID) {

        return getWrapper(dbID.getPropertiesFilePath());
    }

    /**
     * Load the properties file into memory.
     *
     * @throws IOException if the properties file does not exist or couldn't be loaded.
     */
    @Override
    public synchronized void loadProperties() throws IOException {

        if (propertiesFileLocation.startsWith("http:")) { // Parse URL, request file from webpage.

            final URL url = new URL(propertiesFileLocation);
            final InputStreamReader inputReader = new InputStreamReader(url.openStream());

            try {
                properties.load(inputReader);
            }
            finally {
                inputReader.close();
            }
        }
        else { // Try to open the file from disk.

            final File f = new File(propertiesFileLocation);

            if (!f.exists()) { throw new IOException("Properties file doesn't exist at this location (" + propertiesFileLocation + ").");
            // This check is necessary because a file will be created when FileInputStream is created.
            }

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(propertiesFileLocation);
                properties.load(inputStream);
            }
            catch (final FileNotFoundException e) {
                ErrorHandling.exceptionError(e, "Unexpected file not found exception");
            }
            finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        }
    }

    /**
     * Deletes any existing properties file with the given name and creates a new one.
     *
     * @throws IOException if an error occurs while creating a file
     */
    @Override
    public synchronized void createNewFile() throws IOException {

        removePropertiesFile();
        final File f = new File(propertiesFileLocation);

        if (f.getParentFile() != null) {
            final boolean successful = f.getParentFile().mkdirs(); // create any directories specified in the path, if necessary.
            if (!successful) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to create folder. It may already exist.");
            }
        }

        final boolean successful = f.createNewFile(); // create the properties file.
        if (!successful) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to create file. It may already exist.");
        }
    }

    /**
     * Gets the property associated with a given key
     *
     * @param key a key
     * @return the corresponding property
     */
    @Override
    public synchronized String getProperty(final String key) {

        return properties.getProperty(key);
    }

    /**
     * Tests whether the property for a given key is true.
     *
     * @param key a key
     * @return true if the property for the key is true
     */
    @Override
    public synchronized boolean isEnabled(final String key) {

        return TRUE.equals(getProperty(key));
    }

    /**
     * Sets the property for a given key, and saves to the backing file.
     *
     * @param key a key
     * @param value the new value to be associated with the key
     */
    @Override
    public synchronized void setProperty(final String key, final String value) {

        properties.setProperty(key, value);
    }

    /**
     * Sets the property comment.
     *
     * @param propertyComment the comment
     */
    @Override
    public synchronized void setPropertyComment(final String propertyComment) {

        comment = propertyComment;
    }

    private synchronized boolean removePropertiesFile() {

        final File f = new File(propertiesFileLocation);

        return f.delete();
    }

    /**
     * Save the properties file and close it.
     *
     * @throws IOException if the file couldn't be saved.
     */
    @Override
    public synchronized void saveAndClose() throws IOException {

        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(propertiesFileLocation);
            properties.store(outputStream, comment);
        }
        catch (final FileNotFoundException e) {
            ErrorHandling.exceptionError(e, "Unexpected file not found exception");
        }
        finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    public synchronized void saveAndClose(final FileOutputStream outputStream) throws IOException {

        try {
            properties.store(outputStream, comment);
        }
        catch (final FileNotFoundException e) {
            ErrorHandling.exceptionError(e, "Unexpected file not found exception");
        }
        finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
    }

    /**
     * Returns a set of the keys in this properties file.
     *
     * @return a set of the keys in this properties file
     */
    @Override
    public synchronized Set<Object> getKeys() {

        return properties.keySet();
    }

    public String getLocation() {

        return new File(propertiesFileLocation).getAbsolutePath();
    }
}
