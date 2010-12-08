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
import org.h2.util.SortedProperties;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.DatabaseURL;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.test.fixture.DatabaseType;
import org.h2o.util.LocalH2OProperties;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

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

    public static final String DEFAULT_DATABASE_DIRECTORY_PATH = "db_files";
    public static final String DEFAULT_DATABASE_NAME = "database";
    public static final int DEFAULT_TCP_PORT = 9090;

    private static final DiagnosticLevel DEFAULT_DIAGNOSTIC_LEVEL = DiagnosticLevel.FINAL;

    private String databaseName;
    private int tcpPort;
    private int webPort;

    private String databaseDescriptorLocation;
    private String databaseBaseDirectoryPath;
    private DiagnosticLevel diagnosticLevel;

    private Connection connection;
    private H2OLocator locator;
    private Server server;
    private DatabaseType databaseType;

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
     *            <li><em>-f<directory></em>. Optional. Specifies the directory containing the persistent database state. The default is the current working directory.</li>
     *            <li><em>-D<level></em>. Optional. Specifies a diagnostic level from 0 (most detailed) to 6 (least detailed).</li>
     *            <li><em>-M</em>. Optional. Specifies an in-memory database. If specified, the -p, -w and -f flags are ignored.</li>
     *            </ul>
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

        final H2O db = new H2O(args);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting H2O Server Instance.");

        db.startDatabase();
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Starts a new H2O instance using the specified descriptor file to find an existing, running, locator server. This option does not start
     * H2O's web interface.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param databaseDirectoryPath the directory in which database files are stored
     * @param databaseDescriptorLocation the location (file path or URL) of the database descriptor file
     */
    public H2O(final String databaseName, final int tcpPort, final String databaseDirectoryPath, final String databaseDescriptorLocation, final DiagnosticLevel diagnosticLevel) {

        init(databaseName, tcpPort, 0, databaseDirectoryPath, databaseDescriptorLocation, diagnosticLevel);
    }

    /**
     * Starts a local H2O instance with a running TCP server <strong>and web interface</strong>. This will automatically start a local
     * locator file, and doesn't need a descriptor file to run. A descriptor file will be created within the database directory.
     * 
     * @param databaseName the name of the database
     * @param tcpPort the port for this database's TCP server
     * @param webPort the port for this database's web interface
     * @param databaseDirectoryPath the directory in which database files are stored
     */
    public H2O(final String databaseName, final int tcpPort, final int webPort, final String databaseDirectoryPath, final DiagnosticLevel diagnosticLevel) {

        init(databaseName, tcpPort, webPort, databaseDirectoryPath, null, diagnosticLevel);
    }

    public H2O(final String[] args) throws StartupException {

        try {
            init(args);
        }
        catch (final UndefinedDiagnosticLevelException e) {
            throw new StartupException("invalid diagnostic level");
        }
    }

    /**
     * Starts an in-memory database.
     * 
     * @param databaseName
     * @param databaseDescriptorLocation
     * @param diagnosticLevel
     */
    public H2O(final String databaseName, final String databaseBaseDirectoryPath, final String databaseDescriptorLocation, final DiagnosticLevel diagnosticLevel) {

        init(databaseName, databaseDescriptorLocation, diagnosticLevel);
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
    private void init(final String databaseName, final int tcpPort, final int webPort, final String databaseBaseDirectoryPath, final String databaseDescriptorLocation, final DiagnosticLevel diagnosticLevel) {

        this.databaseName = databaseName;
        this.tcpPort = tcpPort;
        this.webPort = webPort;
        this.databaseBaseDirectoryPath = databaseBaseDirectoryPath;
        this.databaseDescriptorLocation = databaseDescriptorLocation;
        this.diagnosticLevel = diagnosticLevel;

        databaseType = DatabaseType.DISK;

        Diagnostic.setLevel(diagnosticLevel);
    }

    private void init(final String databaseName, final String databaseDescriptorLocation, final DiagnosticLevel diagnosticLevel) {

        this.databaseName = databaseName;
        this.databaseDescriptorLocation = databaseDescriptorLocation;
        this.diagnosticLevel = diagnosticLevel;

        databaseType = DatabaseType.MEMORY;

        Diagnostic.setLevel(diagnosticLevel);
    }

    private void init(final String[] args) throws StartupException, UndefinedDiagnosticLevelException {

        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = processDatabaseName(arguments.get("-n"));
        final int tcpPort = processTCPPort(arguments.get("-p"));
        final String databaseDirectoryPath = processDatabaseDirectoryPath(arguments.get("-f"));

        final String databaseDescriptorLocation = processDatabaseDescriptorLocation(arguments.get("-d"));
        final int webPort = processWebPort(arguments.get("-w"));
        final DiagnosticLevel diagnosticLevel = DiagnosticLevel.getDiagnosticLevelFromCommandLineArg(arguments.get("-D"), DEFAULT_DIAGNOSTIC_LEVEL);

        final DatabaseType databaseType = processDatabaseType(arguments.get("-M"));

        switch (databaseType) {
            case MEMORY: {

                init(databaseName, databaseDescriptorLocation, diagnosticLevel);
                break;
            }
            case DISK: {

                init(databaseName, tcpPort, webPort, databaseDirectoryPath, databaseDescriptorLocation, diagnosticLevel);
                break;
            }
            default: {
                ErrorHandling.hardError("unexpected database type");
            }
        }
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

            locator = new H2OLocator(databaseName, locatorPort, true, databaseBaseDirectoryPath);

            databaseDescriptorLocation = locator.start();
        }

        final DatabaseID databaseURL = generateDatabaseURL();

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

    private String processDatabaseName(final String arg) {

        return arg == null ? DEFAULT_DATABASE_NAME : arg;
    }

    private String processDatabaseDirectoryPath(final String arg) {

        return arg == null ? DEFAULT_DATABASE_DIRECTORY_PATH : removeQuotes(arg);
    }

    private String processDatabaseDescriptorLocation(final String arg) {

        return arg == null ? null : removeQuotes(arg);
    }

    private int processTCPPort(final String arg) throws StartupException {

        try {
            return arg == null ? DEFAULT_TCP_PORT : Integer.parseInt(arg);
        }
        catch (final NumberFormatException e) {
            throw new StartupException("Invalid port: " + arg);
        }
    }

    private int processWebPort(final String arg) throws StartupException {

        try {
            return arg == null ? 0 : Integer.parseInt(arg);
        }
        catch (final NumberFormatException e) {
            throw new StartupException("Invalid port: " + arg);
        }
    }

    private DatabaseType processDatabaseType(final String arg) {

        return arg == null ? DatabaseType.DISK : DatabaseType.MEMORY;
    }

    // -------------------------------------------------------------------------------------------------------

    /**
     * Call the H2O server class with the required parameters to initialize the TCP server.
     * 
     * @param databaseURL the database URL
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    private void startServer(final DatabaseID databaseURL) throws SQLException, IOException {

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
     * @throws IOException 
     */
    private void initializeDatabase(final DatabaseID databaseURL) throws SQLException, IOException {

        initializeDatabaseProperties(databaseURL, diagnosticLevel, databaseDescriptorLocation, databaseName);

        // Create a connection so that the database starts up, but don't do anything with it here.
        connection = DriverManager.getConnection(databaseURL.getURL(), PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
    }

    public static void initializeDatabaseProperties(final DatabaseID databaseURL, final DiagnosticLevel diagnosticLevel, final String databaseDescriptorLocation, final String databaseName) throws IOException {

        final LocalH2OProperties properties = new LocalH2OProperties(databaseURL);

        try {
            properties.loadProperties();
        }
        catch (final IOException e) {
            properties.createNewFile();
        }

        // Overwrite these properties regardless of whether properties file exists or not.
        properties.setProperty("diagnosticLevel", diagnosticLevel.toString());
        properties.setProperty("descriptor", databaseDescriptorLocation);
        properties.setProperty("databaseName", databaseName);

        properties.saveAndClose();
    }

    /**
     * Set the primary database URL in the browser to equal the URL of this database.
     * 
     * @param databaseURL the database URL
     * @throws IOException if the server properties cannot be written
     * @throws SQLException if the server properties cannot be opened
     */
    private void setUpWebLink(final DatabaseID databaseURL) throws IOException, SQLException {

        final Properties serverProperties = loadServerProperties();
        final List<String> servers = new LinkedList<String>();
        final String url_as_string = databaseURL.getURL();

        for (int i = 0;; i++) {
            final String data = serverProperties.getProperty(String.valueOf(i));
            if (data == null) {
                break;
            }
            if (!data.contains(url_as_string)) {
                servers.add(data);
            }

            serverProperties.remove(String.valueOf(i));
        }

        int i = 0;
        for (final String server : servers) {
            serverProperties.setProperty(i + "", server);
            i++;
        }

        serverProperties.setProperty(i + "", "QuickStart-H2O-Database|org.h2.Driver|" + url_as_string + "|sa");

        final OutputStream out = FileUtils.openFileOutputStream(getPropertiesFileName(), false);
        serverProperties.store(out, Constants.SERVER_PROPERTIES_TITLE);

        out.close();
    }

    private String getPropertiesFileName() {

        // Store the properties in the user directory.
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

    private DatabaseID generateDatabaseURL() {

        switch (databaseType) {
            case DISK: {
                return new DatabaseID(null, new DatabaseURL(tcpPort, databaseBaseDirectoryPath, databaseName));
            }
            case MEMORY: {
                return new DatabaseID(null, new DatabaseURL(databaseName));
            }
            default: {
                ErrorHandling.hardError("unknown database type");
                return null;
            }
        }
    }
}
