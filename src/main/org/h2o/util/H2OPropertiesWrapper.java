package org.h2o.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.h2o.db.id.DatabaseID;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Wrapper for java properties class, which make it easier to create, load, and save properties.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2OPropertiesWrapper {

    private final Properties properties;
    private final String propertiesFileLocation;

    private String comment = "Properties File";

    private static final String TRUE = "true";

    private static Map<String, H2OPropertiesWrapper> map = new HashMap<String, H2OPropertiesWrapper>();

    /**
     * Creates a properties wrapper for a given file.
     * 
     * @param fileLocation the location of the file containing properties
     */
    private H2OPropertiesWrapper(final String fileLocation) {

        propertiesFileLocation = fileLocation;
        properties = new Properties();
    }

    public static synchronized H2OPropertiesWrapper getWrapper(final String fileLocation) {

        H2OPropertiesWrapper wrapper = map.get(fileLocation);
        if (wrapper == null) {
            wrapper = new H2OPropertiesWrapper(fileLocation);
            map.put(fileLocation, wrapper);
        }
        return wrapper;
    }

    /**
     * @param dbID the URL of this database instance. This is used to name and locate the properties file for this database on disk.
     */
    public static synchronized H2OPropertiesWrapper getWrapper(final DatabaseID dbID) {

        return getWrapper(dbID.getPropertiesFilePath());
    }

    /**
     * Load the properties file into memory.
     * 
     * @throws IOException if the properties file does not exist or couldn't be loaded.
     */
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
     * @throws IOException
     */
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
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to create folder. It may already exist.");
        }
    }

    /**
     * Gets the property associated with a given key
     * 
     * @param key a key
     * @return the corresponding property
     */
    public synchronized String getProperty(final String key) {

        return properties.getProperty(key);
    }

    /**
     * Tests whether the property for a given key is true.
     * 
     * @param key a key
     * @return true if the property for the key is true
     */
    public synchronized boolean isEnabled(final String key) {

        return TRUE.equals(getProperty(key));
    }

    /**
     * Sets the property for a given key, and saves to the backing file.
     * 
     * @param key a key
     * @param value the new value to be associated with the key
     */
    public synchronized void setProperty(final String key, final String value) {

        properties.setProperty(key, value);
    }

    /**
     * Sets the property comment.
     * 
     * @param propertyComment the comment
     */
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
     * @throws IOException
     *             if the file couldn't be saved.
     */
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

    /**
     * Returns a set of the keys in this properties file.
     * 
     * @return a set of the keys in this properties file
     */
    public synchronized Set<Object> getKeys() {

        return properties.keySet();
    }

    public synchronized String getPropertiesFileLocation() {

        return propertiesFileLocation;
    }
}
