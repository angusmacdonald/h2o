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
public class H2OLocator {

    private static final long SHUTDOWN_CHECK_DELAY = 2000;

    private final String databaseName;
    private final String port;
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
     *            </ul>
     *            <em>Example: StartLocatorServer -lMyFirstDatabase -p20000 -d</em> . This creates a new locator server for the database
     *            called <em>MyFirstDatabase</em> on port 20000, and creates a descriptor file specifying this in the local folder.
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = arguments.get("-n");
        final String port = arguments.get("-p");
        final boolean createDescriptor = arguments.containsKey("-d");
        String descriptorFileDirectory = arguments.get("-f"); // e.g. "db_data/wrapper"
        descriptorFileDirectory = removeParenthesis(descriptorFileDirectory);

        final H2OLocator locator = new H2OLocator(databaseName, Integer.parseInt(port), createDescriptor, descriptorFileDirectory);

        locator.start();
    }

    public H2OLocator(final String databaseName, final int port, final boolean createDescriptor, final String configurationDirectory) {

        Diagnostic.setLevel(DiagnosticLevel.FINAL);

        this.databaseName = databaseName;
        this.port = port + "";
        this.createDescriptor = createDescriptor;
        this.configurationDirectory = configurationDirectory;
    }

    public String start() {

        final String locatorLocation = NetUtils.getLocalAddress() + ":" + port;
        String descriptorFilePath = null;

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting locator server.");
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Port: " + port);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Name: " + databaseName);
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Locator location: " + locatorLocation);

        if (!createDescriptor) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "IMPORTANT NOTE: The location of this server must appear in the database descriptor file.");
        }
        else {
            try {
                descriptorFilePath = createDescriptorFile(locatorLocation);
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to create descriptor file. If you manually create this file the location of this server must be included.");
            }
        }

        server = new LocatorServer(Integer.parseInt(port), databaseName, configurationDirectory);
        server.start();

        return descriptorFilePath;
    }

    public void shutdown() {

        server.setRunning(false);

        while (!server.isFinished()) {
            try {
                Thread.sleep(SHUTDOWN_CHECK_DELAY);
            }
            catch (final InterruptedException e) {
                // Ignore and carry on.
            }
        }
    }

    private String createDescriptorFile(final String locatorLocation) throws FileNotFoundException, IOException {

        final String descriptorFilePath = configurationDirectory + File.separator + databaseName + ".h2od";

        File f = new File(configurationDirectory);

        if (!f.exists()) {
            final boolean successful = f.mkdir();

            if (!successful) {
                ErrorHandling.errorNoEvent("Failed to create new directory for locator file. It may already exist.");
            }
        }

        f = new File(descriptorFilePath);
        try {
            f.createNewFile();
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        final FileOutputStream fos = new FileOutputStream(descriptorFilePath);

        final Properties descriptor = new Properties();

        descriptor.setProperty("databaseName", databaseName);
        descriptor.setProperty("locatorLocations", locatorLocation);

        descriptor.store(fos, "H2O Database Descriptor file.");

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "\n\tThe descriptor file for this database has been created at: " + f.getAbsolutePath() + "\n\t" + "The location of this file must be accessible to new H2O database instances (you can put it in your webspace and link to the URL).");

        fos.close();

        return descriptorFilePath;
    }

    private static String removeParenthesis(String text) {

        if (text == null) { return null; }

        if (text.startsWith("'") && text.endsWith("'")) {
            text = text.substring(1, text.length() - 1);
        }
        return text;
    }
}
