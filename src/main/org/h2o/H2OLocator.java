/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.h2.util.NetUtils;
import org.h2o.locator.server.LocatorServer;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * This class starts a new H2O locator server. If this is the first locator server to be started an H2O database descriptor file can also be
 * created. This file specifies the location of the newly created locator server, and is used to allow new database instances to make
 * contact.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2OLocator extends H2OCommon {

    private static final DiagnosticLevel DEFAULT_DIAGNOSTIC_LEVEL = DiagnosticLevel.FINAL;

    private final String databaseName;
    private final int locatorPort;
    private final String configurationDirectory;
    private final boolean createDescriptor;

    private LocatorServer server;

    /**
     * Starts an H2O Locator server.
     * 
     * @param args
     *            <ul>
     *            <li><em>-n</em>. Specify the name of the database for which this locator server is running.</li>
     *            <li><em>-p</em>. The port on which the locator server is to run.</li>
     *            <li><em>-d</em>. Optional. Create a database descriptor file which includes the location of this locator server.
     *            <li><em>-f</em>. Optional. Specify the folder into which the descriptor file will be generated. The default is the folder
     *            this class is being run from. If this option is not chosen you must add the location of this locator server to an existing
     *            descriptor file.</li>
     *            <li><em>-D<level></em>. Optional. Specifies a diagnostic level from 0 (most detailed) to 6 (least detailed).</li>
     *            </ul>
     *            <em>Example: StartLocatorServer -nMyFirstDatabase -p20000 -d</em> . This creates a new locator server for the database
     *            called <em>MyFirstDatabase</em> on port 20000, and creates a descriptor file specifying this in the local folder.
     *            
     * @throws StartupException if an error occurs while parsing the command line arguments
     * @throws IOException if the locator server cannot be started using the given port
     */
    public static void main(final String[] args) throws StartupException, IOException {

        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = arguments.get("-n");
        final String locatorPortString = arguments.get("-p");
        final boolean createDescriptor = arguments.containsKey("-d");
        final String configurationDirectory = removeQuotes(arguments.get("-f"));

        final DiagnosticLevel diagnosticLevel = processDiagnosticLevel(arguments.get("-D"), DEFAULT_DIAGNOSTIC_LEVEL);
        Diagnostic.setLevel(diagnosticLevel);

        final H2OLocator locator = new H2OLocator(databaseName, Integer.parseInt(locatorPortString), createDescriptor, configurationDirectory);
        locator.start();
    }

    public H2OLocator(final String databaseName, final int locatorPort, final boolean createDescriptor, final String configurationDirectory) {

        this.databaseName = databaseName;
        this.locatorPort = locatorPort;
        this.createDescriptor = createDescriptor;
        this.configurationDirectory = configurationDirectory;
    }

    private static String getConfigurationDirectoryPath(final String databaseBaseDirectoryPath, final String databaseName, final int port) {

        return databaseBaseDirectoryPath + File.separator + databaseName + port;
    }

    public String start() throws IOException {

        final String locatorLocation = NetUtils.getLocalAddress() + ":" + locatorPort;
        String descriptorFilePath = null;

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting locator server.");
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Port: " + locatorPort);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Name: " + databaseName);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Locator location: " + locatorLocation);

        if (!createDescriptor) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "IMPORTANT NOTE: The location of this server must appear in the database descriptor file.");
        }
        else {
            try {
                descriptorFilePath = createDescriptorFile(locatorLocation);
            }
            catch (final IOException e) {
                ErrorHandling.exceptionError(e, "Failed to create descriptor file. If you manually create this file the location of this server must be included.");
            }
        }

        server = new LocatorServer(locatorPort, databaseName, configurationDirectory);
        server.start();

        return descriptorFilePath;
    }

    public void shutdown() {

        server.shutdown();
    }

    private String createDescriptorFile(final String locatorLocation) throws FileNotFoundException, IOException {

        final String descriptorFilePath = configurationDirectory + File.separator + databaseName + ".h2od";

        File f = new File(configurationDirectory);

        if (!f.exists()) {
            if (!f.mkdirs()) { throw new IOException("Could not create directory for descriptor file"); }
        }

        f = new File(descriptorFilePath);
        if (!f.exists()) {
            if (!f.createNewFile()) { throw new IOException("Could not create descriptor file"); }
        }

        final FileOutputStream output_stream = new FileOutputStream(descriptorFilePath);

        final Properties descriptor = new Properties();

        descriptor.setProperty("databaseName", databaseName);
        descriptor.setProperty("locatorLocations", locatorLocation);

        descriptor.store(output_stream, "H2O Database Descriptor file.");

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "\n\tThe descriptor file for this database has been created at: " + f.getAbsolutePath() + "\n\t" + "The location of this file must be accessible to new H2O database instances (you can put it in your webspace and link to the URL).");

        output_stream.close();

        return descriptorFilePath;
    }

    private static String removeQuotes(String text) {

        if (text == null) { return null; }

        if (text.startsWith("'") && text.endsWith("'")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }
}
