package org.h2o.eval;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.FileUtil;
import uk.ac.standrews.cs.nds.util.NetworkUtil;

public class CoordinationScriptRunner {

    /**
     * Executes a co-ordinator script based on the provided parameters.
     * @param args
     *            <ul>
     *            <li><em>-n<name></em>. The name of the database system (i.e. the name of the database in the descriptor file, the global system).</li>
     *            <li><em>-w<name></em>. A list of all the locations where worker nodes may be running, delimited by a semi-colon (e.g. "hostname1;hostname2;hostname3"). It is assumed that
     *            an instance will be started on the local instance, so this hostname does not need to be included.</li>
     *            <li><em>-c<name></em>. Where the co-ordinator script can be found.</li>
     *            <li><em>-f<name></em>. Where results should be written.</li>
     *            <li><em>-t<name></em>. Optional (boolean). Whether to terminate any existing instances running at all workers (including stored state), before doing anything else.</li>
     *            <li><em>-d<name></em>. Optional (boolean). Whether to create a number of local worker instances to facilitate testing.</li>
     *            <li><em>-r<name></em>. Optional. The system-wide replication factor for user tables.</li>
     *            li><em>-r<name></em>. Optional. The time slice to use when displaying data in timeSlice.csv (results data).</li>
     *            </ul>
     * @throws StartupException Thrown if a required parameter was not specified.
     * @throws IOException 
     * @throws AlreadyBoundException 
     * @throws SQLException 
     */
    public static void main(final String[] args) throws StartupException, IOException, AlreadyBoundException, SQLException {

        Diagnostic.setLevel(DiagnosticLevel.FINAL);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Co-ordinator Script Runner version 0.4");

        /*
         * Parse parameters.
         */
        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = processDatabaseName(arguments.get("-n"));
        final String[] workerLocationsStr = processWorkerLocations(arguments.get("-w"));

        final List<InetAddress> workerLocationsInet = convertFromStringToInetAddress(workerLocationsStr);
        workerLocationsInet.add(NetworkUtil.getLocalIPv4Address());

        final Integer replicationFactor = processInteger(arguments.get("-r"));

        final Integer timeSlicePeriod = processInteger(arguments.get("-t"));

        final boolean startWorkersLocallyForTesting = processBoolean(arguments.get("-d"));

        throwExceptionIfNull(replicationFactor, "The replication factor of the database system must be specified with the -r argument.");

        final String coordinatorScriptLocation = arguments.get("-c");

        throwExceptionIfNull(coordinatorScriptLocation, "The co-ordinator script location must be specified with the -c argument.");

        String resultsFolderLocation = arguments.get("-f");

        if (resultsFolderLocation == null) {
            resultsFolderLocation = Coordinator.DEFAULT_RESULTS_FOLDER_LOCATION;
        }

        final List<String> script = FileUtil.readAllLines(coordinatorScriptLocation);

        ScriptRunner.runCoordinationScript(databaseName, workerLocationsInet, replicationFactor, startWorkersLocallyForTesting, script, resultsFolderLocation, coordinatorScriptLocation, timeSlicePeriod);

        System.exit(0);
    }

    private static boolean processBoolean(final String arg) {

        return arg == null ? false : Boolean.valueOf(arg);
    }

    private static void throwExceptionIfNull(final Object obj, final String errorMessage) throws StartupException {

        if (obj == null) { throw new StartupException(errorMessage); }

    }

    public static InetAddress getLocalHostname() throws StartupException {

        InetAddress host = null;
        try {
            host = NetworkUtil.getLocalIPv4Address();
        }
        catch (final UnknownHostException e1) {
            throw new StartupException("Couldn't create local InetAddress.");
        }
        return host;
    }

    private static List<InetAddress> convertFromStringToInetAddress(final String[] hostnames) {

        final List<InetAddress> inetAddresses = new LinkedList<InetAddress>();

        for (final String hostname : hostnames) {
            if (hostname != null && !hostname.equals("")) {
                try {
                    inetAddresses.add(InetAddress.getByName(hostname));
                }
                catch (final UnknownHostException e) {
                    ErrorHandling.errorNoEvent("Failed to convert from hostname '" + hostname + "' to InetAddress: " + e.getMessage());
                }
            }
        }

        return inetAddresses;
    }

    private static int processInteger(final String integer) throws StartupException {

        if (integer != null) {
            return Integer.parseInt(integer);
        }
        else {
            throw new StartupException("Number of instances to start was not specified.");
        }
    }

    /**
     * 
     * @param locations Delimited by semi-colons.
     * @return
     * @throws StartupException 
     */
    private static String[] processWorkerLocations(final String locations) throws StartupException {

        if (locations != null) {
            return locations.split(";");
        }
        else {
            throw new StartupException("The locations of worker instances were not specified.");
        }
    }

    private static String processDatabaseName(final String arg) {

        return arg == null ? Coordinator.DEFAULT_DATABASE_NAME : arg;
    }

}
