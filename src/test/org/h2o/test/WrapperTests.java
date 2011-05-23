/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.h2o.db.manager.PersistentSystemTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.JavaProcessDescriptor;
import uk.ac.standrews.cs.nds.madface.PlatformDescriptor;
import uk.ac.standrews.cs.nds.madface.ProcessManager;
import uk.ac.standrews.cs.nds.madface.exceptions.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

import com.mindbright.ssh2.SSH2Exception;

public class WrapperTests {

    private Process locatorProcess = null;

    private Process databaseProcess = null;

    private static final String defaultLocation = "db_data";

    private static final String databaseName = "TestDB";

    private static int databasePort = 0;

    @Before
    public void setUp() {

        deleteDatabase(databasePort);
    }

    @After
    public void tearDown() {

        deleteDatabase(databasePort);
    }

    @Test
    public void startSingleDatabaseInstance() throws InterruptedException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        killExistingProcessesIfNotOnWindows();

        Diagnostic.setLevel(DiagnosticLevel.INIT);

        final int locatorPort = 29990;

        try {
            /*
             * Start the locator server.
             */
            final List<String> locatorArgs = new LinkedList<String>();
            locatorArgs.add("-p" + locatorPort);
            locatorArgs.add("-n" + databaseName);
            locatorArgs.add("-d");
            locatorArgs.add("-f'" + defaultLocation + "'");

            locatorProcess = new ProcessManager().runProcess(new JavaProcessDescriptor().classToBeInvoked(H2OLocator.class).args(locatorArgs));

            Thread.sleep(1000);

            startDatabaseInSeparateProcess();
            databasePort = H2O.getDatabasesJDBCPort(defaultLocation, databaseName, 10);

            /*
             * Make application connection to the database.
             */
            final String databaseLocation = extractDatabaseLocation(databasePort, databaseName, defaultLocation);

            final String databaseURL = createDatabaseURL(databasePort, NetUtils.getLocalAddress(), databaseLocation);

            testDatabaseAccess(databaseURL);

        }
        finally {
            /*
             * Kill off processes.
             */
            if (locatorProcess != null) {
                locatorProcess.destroy();
            }
            if (databaseProcess != null) {
                databaseProcess.destroy();
            }
        }
    }

    private void killExistingProcessesIfNotOnWindows() {

        final HostDescriptor host_descriptor = new HostDescriptor();
        if (!host_descriptor.getPlatform().getName().equals(PlatformDescriptor.NAME_WINDOWS)) {
            try {
                host_descriptor.getProcessManager().killMatchingProcesses(H2OLocator.class.getSimpleName());
                host_descriptor.getProcessManager().killMatchingProcesses(H2O.class.getSimpleName());
            }
            catch (final IOException e1) {

                e1.printStackTrace();
                //Error thrown if no matching processes were found.
            }
            catch (final SSH2Exception e) {
                ErrorHandling.error("unexpected exception on local host");
            }
            catch (final TimeoutException e) {
                ErrorHandling.error("unexpected exception on local host");
            }
            catch (final UnknownPlatformException e) {
                ErrorHandling.error("unexpected exception on local host");
            }
        }
    }

    /**
     * Query the database at the specified location.
     *
     * @param databaseURL
     */
    private void testDatabaseAccess(final String databaseURL) {

        Connection conn = null;
        Statement stat = null;
        try {
            conn = DriverManager.getConnection(databaseURL, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            stat = conn.createStatement();
            stat.executeUpdate("CREATE TABLE TEST (ID INT);");
            stat.executeUpdate("INSERT INTO TEST VALUES(7);");
            final ResultSet rs = stat.executeQuery("SELECT * FROM TEST;");

            if (rs.next()) {
                assertEquals(7, rs.getInt(1));
            }
            else {
                fail("Couldn't query database.");
            }
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Unexpected IO Exception.");
        }
        finally {
            try {
                if (conn != null) {
                    conn.close();
                }

                if (stat != null) {
                    stat.close();
                }
            }
            catch (final SQLException e) {
            }
        }
    }

    /**
     * Start a new database instance on the port specified.
     *
     * @param databasePort
     * @param databaseProcess
     * @throws TimeoutException
     * @throws SSH2Exception
     * @throws UnknownPlatformException
     * @throws IOException
     * @throws InterruptedException 
     */
    private void startDatabaseInSeparateProcess() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException, InterruptedException {

        /*
         * Start the database instance.
         */
        final List<String> databaseArgs = new LinkedList<String>();

        databaseArgs.add("-n" + databaseName);
        databaseArgs.add("-d'" + defaultLocation + File.separator + databaseName + ".h2od'");
        databaseArgs.add("-f'" + defaultLocation + "'");

        databaseProcess = new ProcessManager().runProcess(new JavaProcessDescriptor().classToBeInvoked(H2O.class).args(databaseArgs));
    }

    private String extractDatabaseLocation(final int databasePort, final String databaseName, String defaultLocation) {

        if (defaultLocation != null) {
            if (!defaultLocation.endsWith("/") && !defaultLocation.endsWith("\\")) { // add a trailing slash if it isn't already there.
                defaultLocation = defaultLocation + "/";
            }
        }
        final String databaseLocation = (defaultLocation != null ? defaultLocation : "") + databaseName;
        return databaseLocation;
    }

    private static String createDatabaseURL(final int port, final String hostname, final String databaseLocation) {

        return "jdbc:h2:tcp://" + hostname + ":" + port + "/" + databaseLocation;
    }

    private void deleteDatabase(final int databasePort) {

        try {
            DeleteDbFiles.execute(defaultLocation, databaseName + databasePort, true);
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
    }
}
