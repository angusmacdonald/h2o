/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2.util.FileUtils;
import org.h2.util.NetUtils;
import org.h2.util.SortedProperties;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * This class starts an instance of an H2O database. It can be run from the command line (see the main method for applicable arguments), or
 * programmatically. The instance can start in standalone mode, or as part of a more customisable set-up:
 * <ul>
 * <li>Standalone start-up. A single H2O instance will be started with its own locator server. This requires minimum options at
 * initialisation. This option restricts the H2O locator server to the same process as the H2O instance, and is not recommended for
 * multi-machine database set-ups.</li>
 * <li>Custom start-up. An H2O instance will be started and will connect to the database system via a database descriptor file. This file
 * should already exist, and should specify the location of an H2O Locator server. The locator server should already be running at the
 * address specified. For information on how to create a descriptor file and naming server see the {@link H2OLocator} class.</li>
 * </ul>
 * <p>
 * If the H2O web interface is required please use one of the constructors that requires a web port as a parameter. If this interface is not
 * required, use another constructor.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2O {

    private final String databaseName;
    private final int tcpPort;
    private final int webPort;

    private String databaseDescriptorLocation;
    private String databaseBaseDirectoryPath;

    private Connection connection;
    private H2OLocator locator;
    private Server server;

    // -------------------------------------------------------------------------------------------------------

    /**
     * Starts a H2O database instance.
     * 
     * @param args
     *            <ul>
     *            <li><em>-n<name></em>. The name of the database.</li>
     *            <li><em>-p<port></em>. The port on which the database's TCP server is to run.</li>
     *            <li><em>-w<port></em>. Optional. Specifies that a web port should be opened and the web interface should be started.</li>
     *            <li><em>-d<descriptor></em>. Optional. Specifies the URL or local file path of the database descriptor file. If not specified the database will create a new descriptor file in the database directory.</li>
     *            <li><em>-f</em>. Optional. Specifies the directory containing the persistent database state. The default is the current working directory.</li>
     *            </p>
     *            <p>
     *            <em>Example: java H2O -nMyFirstDatabase -p9999 -d'config\MyFirstDatabase.h2od'</em>. This creates a new
     *            database instance for the database called <em>MyFirstDatabase</em> on port 9999, and initializes by connecting to the
     *            locator files specified in the file <em>'config\MyFirstDatabase.h2od'</em>.
     *            
     * @throws StartupException if an error occurs while parsing the command line arguments
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    public static void main(final String[] args) throws StartupException, IOException, SQLException {

        Diagnostic.setLevel(DiagnosticLevel.FINAL);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting H2O Server Instance.");

        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);
        final H2O db = initialiseFromCommandLineArguments(arguments);

        db.startDatabase();
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Start a new H2O instance using the specified descriptor file to find an existing, running, locator server. This also starts H2O's web interface.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param webPort the port for this database's web interface
     * @param databaseBaseDirectoryPath the directory in which database files are stored
     * @param databaseDescriptorLocation the location (file path or URL) of the database descriptor file
     */
    public H2O(final String databaseName, final int tcpPort, final int webPort, final String databaseBaseDirectoryPath, final String databaseDescriptorLocation) {

        this.databaseName = databaseName;
        this.tcpPort = tcpPort;
        this.webPort = webPort;
        this.databaseDescriptorLocation = databaseDescriptorLocation;
        this.databaseBaseDirectoryPath = databaseBaseDirectoryPath;
    }

    /**
     * Start a new H2O instance using the specified descriptor file to find an existing, running, locator server. This option does not start
     * H2O's web interface.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param databaseDirectoryPath the directory in which database files are stored
     * @param databaseDescriptorLocation the location (file path or URL) of the database descriptor file
     */
    public H2O(final String databaseName, final int tcpPort, final String databaseDirectoryPath, final String databaseDescriptorLocation) {

        this(databaseName, tcpPort, 0, databaseDirectoryPath, databaseDescriptorLocation);
    }

    /**
     * Start a local H2O instance with a running TCP server <strong>and web interface </strong>. This will automatically start a local
     * locator file, and doesn't need a descriptor file to run. A descriptor file will be created in the configuration directory.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param webPort the port for this database's web interface
     * @param databaseDirectoryPath the directory in which database files are stored
     */
    public H2O(final String databaseName, final int tcpPort, final int webPort, final String databaseDirectoryPath) {

        this(databaseName, tcpPort, webPort, databaseDirectoryPath, null);
    }

    /**
     * Start a local H2O instance with a running TCP server, <strong>but without a web interface</strong>. This will automatically start a
     * local locator file, and doesn't need a descriptor file to run. A descriptor file will be created in the configuration directory.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param databaseDirectoryPath the directory in which database files are stored
     */
    public H2O(final String databaseName, final int tcpPort, final String databaseDirectoryPath) {

        this(databaseName, tcpPort, 0, databaseDirectoryPath, null);
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Starts up an H2O server and initializes the database.
     * 
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    public void startDatabase() throws SQLException, IOException {

        if (databaseDescriptorLocation == null) {

            // A new locator server should be started.
            final int locatorPort = tcpPort + 1;

            locator = new H2OLocator(databaseName, databaseBaseDirectoryPath, locatorPort, tcpPort, true);

            databaseDescriptorLocation = locator.start();
        }

        final String databaseURL = generateDatabaseURL();

        startServer(databaseURL);
        initializeDatabase(databaseURL);
    }

    /**
     * Shuts down the H2O server.
     * @throws SQLException if an error occurs while shutting down the H2O server
     */
    public void shutdown() throws SQLException {

        if (connection != null) {
            connection.close();
        }

        final Collection<Database> dbs = Engine.getInstance().getAllDatabases();

        for (final Database db : dbs) {

            if (db.getShortName().equalsIgnoreCase(databaseName + tcpPort) || db.getShortName().equalsIgnoreCase(Constants.MANAGEMENT_DB_PREFIX + tcpPort)) {
                db.close(true);
                db.shutdownImmediately();
            }
        }

        if (locator != null) {
            locator.shutdown();
        }

        shutdownServer();
    }

    /**
     * Deletes the persistent database state.
     * @throws SQLException if the state cannot be deleted
     */
    public void deletePersistentState() throws SQLException {

        DeleteDbFiles.execute(databaseBaseDirectoryPath, databaseName + tcpPort, true);
    }

    // -------------------------------------------------------------------------------------------------------

    private static H2O initialiseFromCommandLineArguments(final Map<String, String> arguments) throws StartupException {

        // Get required command line arguments.
        final String databaseName = arguments.get("-n");
        final String tcpPortString = arguments.get("-p");

        if (databaseName == null || tcpPortString == null) { throw new StartupException("One of the required command line arguments was not supplied. Please check the documentation to ensure you have included all necessary arguments"); }

        // Get optional command line arguments.
        String databaseDirectoryPath = arguments.get("-f");
        String databaseDescriptorLocation = arguments.get("-d");
        final String webPortString = arguments.get("-w");

        int tcpPort = 0;
        int webPort = 0;

        try {
            tcpPort = Integer.parseInt(tcpPortString);

            if (webPortString != null) {
                webPort = Integer.parseInt(webPortString);
            }
        }
        catch (final NumberFormatException e) {
            throw new StartupException("Invalid port specified.");
        }

        if (databaseDescriptorLocation != null) {
            databaseDescriptorLocation = removeQuotes(databaseDescriptorLocation);
        }

        if (databaseDirectoryPath != null) {
            databaseDirectoryPath = removeQuotes(databaseDirectoryPath);
        }

        return new H2O(databaseName, tcpPort, webPort, databaseDirectoryPath, databaseDescriptorLocation);
    }

    private String generateDatabaseURL() {

        // Add a trailing slash if it isn't already there.
        if (databaseBaseDirectoryPath != null && !databaseBaseDirectoryPath.endsWith("/") && !databaseBaseDirectoryPath.endsWith("\\")) {
            databaseBaseDirectoryPath = databaseBaseDirectoryPath + "/";
        }

        final String hostname = NetUtils.getLocalAddress();
        final String location = (databaseBaseDirectoryPath != null ? databaseBaseDirectoryPath : "") + databaseName + tcpPort;

        final String databaseURL = createDatabaseURL(tcpPort, hostname, location);

        // Display to user.
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Database Name: " + databaseName);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Port: " + tcpPort);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Hostname: " + hostname);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Generated JDBC URL: " + databaseURL);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Specified Descriptor File Location: " + databaseDescriptorLocation);

        return databaseURL;
    }

    /**
     * Call the H2O server class with the required parameters to initialize the TCP server.
     * 
     * @param databaseURL the database URL
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    private void startServer(final String databaseURL) throws SQLException, IOException {

        final List<String> h2oArgs = new LinkedList<String>(); // Arguments to be passed to the H2 server.
        h2oArgs.add("-tcp");

        h2oArgs.add("-tcpPort");
        h2oArgs.add(String.valueOf(tcpPort));

        h2oArgs.add("-tcpAllowOthers"); // allow remote connections.
        h2oArgs.add("-webAllowOthers");

        // Web Interface.

        if (webPort != 0) {
            h2oArgs.add("-web");
            h2oArgs.add("-webPort");
            h2oArgs.add(String.valueOf(webPort));
            h2oArgs.add("-browser");
        }

        // Set URL to be displayed in browser.
        setUpWebLink(databaseURL);

        server = new Server();
        server.run(h2oArgs.toArray(new String[0]), System.out);
    }

    private void shutdownServer() {

        server.shutdown();
    }

    /**
     * Connects to the server and initializes the database at a particular location on disk.
     * 
     * @param databaseURL
     * @throws SQLException 
     */
    private void initializeDatabase(final String databaseURL) throws SQLException {

        final LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL(databaseURL));

        try {
            properties.loadProperties();
        }
        catch (final IOException e) {
            properties.createNewFile();
            properties.setProperty("diagnosticLevel", DiagnosticLevel.NONE.toString());
        }

        // Overwrite these properties regardless of whether properties file exists or not.
        properties.setProperty("descriptor", databaseDescriptorLocation);
        properties.setProperty("databaseName", databaseName);

        properties.saveAndClose();

        // Create a connection so that the database starts up, but don't do anything with it here.
        connection = DriverManager.getConnection(databaseURL, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
    }

    /**
     * Set the primary database URL in the browser to equal the URL of this database.
     * 
     * @param databaseURL the database URL
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    private void setUpWebLink(final String databaseURL) throws IOException, SQLException {

        final Properties serverProperties = loadServerProperties();
        final List<String> servers = new LinkedList<String>();

        for (int i = 0;; i++) {
            final String data = serverProperties.getProperty(String.valueOf(i));
            if (data == null) {
                break;
            }
            if (!data.contains(databaseURL)) {
                servers.add(data);
            }

            serverProperties.remove(String.valueOf(i));
        }

        int i = 0;
        for (final String server : servers) {
            serverProperties.setProperty(i + "", server);
            i++;
        }

        serverProperties.setProperty(i + "", "QuickStart-H2O-Database|org.h2.Driver|" + databaseURL + "|sa");

        final OutputStream out = FileUtils.openFileOutputStream(getPropertiesFileName(), false);
        serverProperties.store(out, Constants.SERVER_PROPERTIES_TITLE);

        out.close();
    }

    private String getPropertiesFileName() {

        // store the properties in the user directory
        return FileUtils.getFileInUserHome(Constants.SERVER_PROPERTIES_FILE);
    }

    private Properties loadServerProperties() {

        final String fileName = getPropertiesFileName();
        try {
            return SortedProperties.loadProperties(fileName);
        }
        catch (final IOException e) {
            return new Properties();
        }
    }

    private static String removeQuotes(String text) {

        if (text == null) { return null; }

        if (text.startsWith("'") && text.endsWith("'")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }

    private static String createDatabaseURL(final int port, final String hostname, String databaseLocation) {

        if (!databaseLocation.startsWith("/")) {
            databaseLocation = "/" + databaseLocation;
        }
        return "jdbc:h2:tcp://" + hostname + ":" + port + databaseLocation;
    }
}
