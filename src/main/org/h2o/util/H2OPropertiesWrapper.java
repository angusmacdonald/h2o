package org.h2o.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Wrapper for java properties class, which make it easier to create, load, and save properties.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2OPropertiesWrapper {

    private final Properties properties;

    private final String propertiesFileLocation;

    private FileOutputStream outputStream = null;

    private FileInputStream inputStream = null;

    private String comment = "Properties File";

    private static final String TRUE = "true";

    /**
     * Creates a properties wrapper for a given file.
     * 
     * @param fileLocation
     *            the location of the file containing properties
     */
    public H2OPropertiesWrapper(final String fileLocation) {

        propertiesFileLocation = fileLocation;
        properties = new Properties();
    }

    /**
     * Load the properties file into memory.
     * 
     * @throws IOException
     *             if the properties file does not exist or couldn't be loaded.
     */
    public void loadProperties() throws IOException {

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

            if (inputStream == null) {
                try {
                    inputStream = new FileInputStream(propertiesFileLocation);
                }
                catch (final FileNotFoundException e) {
                    ErrorHandling.exceptionError(e, "Unexpected file not found exception");
                }
            }

            properties.load(inputStream);
        }
    }

    /**
     * Deletes any existing properties file with the given name and creates a new one.
     * 
     * @throws IOException
     */
    public void createNewFile() throws IOException {

        removePropertiesFile();
        final File f = new File(propertiesFileLocation);

        if (f.getParentFile() != null) {
            f.getParentFile().mkdirs(); // create any directories specified in the path, if necessary.

        }

        f.createNewFile(); // create the properties file.

        inputStream = new FileInputStream(propertiesFileLocation);
    }

    /**
     * Gets the property associated with a given key
     * 
     * @param key
     *            a key
     * @return the corresponding property
     */
    public String getProperty(final String key) {

        return properties.getProperty(key);
    }

    /**
     * Tests whether the property for a given key is true.
     * 
     * @param key
     *            a key
     * @return true if the property for the key is true
     */
    public boolean isEnabled(final String key) {

        return TRUE.equals(getProperty(key));
    }

    /**
     * Sets the property for a given key, and saves to the backing file.
     * 
     * @param key
     *            a key
     * @param value
     *            the new value to be associated with the key
     */
    public void setProperty(final String key, final String value) {

        properties.setProperty(key, value);
    }

    /**
     * Sets the property comment.
     * 
     * @param propertyComment
     *            the comment
     */
    public void setPropertyComment(final String propertyComment) {

        comment = propertyComment;
    }

    private boolean removePropertiesFile() {

        try {
            if (inputStream != null) {
                inputStream.close();
            }
            inputStream = null;

            if (outputStream != null) {
                outputStream.close();
            }
            outputStream = null;
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        final File f = new File(propertiesFileLocation);

        return f.delete();
    }

    /**
     * Save the properties file and close it.
     * 
     * @throws IOException
     *             if the file couldn't be saved.
     */
    public void saveAndClose() throws IOException {

        if (outputStream == null) {
            outputStream = new FileOutputStream(propertiesFileLocation);
        }

        properties.store(outputStream, comment);

        if (inputStream != null) {
            inputStream.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
    }

    /**
     * Returns a set of the keys in this properties file.
     * 
     * @return a set of the keys in this properties file
     */
    public Set<Object> getKeys() {

        return properties.keySet();
    }

}
