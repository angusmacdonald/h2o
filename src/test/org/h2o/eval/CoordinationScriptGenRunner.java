package org.h2o.eval;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2o.eval.script.coord.CoordinationScriptGenerator;
import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableClustering.Clustering;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.coord.specification.WorkloadType.LinkToTableLocation;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.NetworkUtil;

public class CoordinationScriptGenRunner {

    /**
     * Creates a new co-ordination script, then executes it with the given parameters.
     * @param args
     *            <ul>
     *            <li><em>-r<name></em>. Runtime of the script.</li>
     *            <li><em>-p<name></em>. Probability of failure for each machine.</li>
     *            <li><em>-f<name></em>. Frequency of failure.</li>
     *            <li><em>-n<name></em>. Number of machines to create (incl. machine 0).</li>
     *            <li><em>-t<name></em>. Number of tables.</li>
     *            <li><em>-q<name></em>. Number of queries per transaction.</li>
     *            <li><em>-m<name></em>. A list of all the locations where worker nodes may be running, delimited by a semi-colon (e.g. "hostname1;hostname2;hostname3"). It is assumed that
     *            an instance will be started on the local instance, so this hostname does not need to be included.</li>
     *            <li><em>-l<name></em>. Where results should be written.</li>
     *            <li><em>-t<name></em>. Optional (boolean). Whether to terminate any existing instances running at all workers (including stored state), before doing anything else.</li>
     *            <li><em>-d<name></em>. Optional (boolean). Whether to create a number of local worker instances to facilitate testing.</li>
     *            <li><em>-R<name></em>. Optional. The system-wide replication factor for user tables.</li>
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

        //Co-ordination script generation parameters:
        final long runtime = processLong(arguments.get("-r"));
        final double probabilityOfFailure = processDouble("-p");
        final long frequencyOfFailure = processLong("-f");
        final int numberOfMachines = processInteger("-n");
        final int numberOfTables = processInteger("-t");
        final int numberOfQueriesPerTransaction = processInteger("-q");

        //Runner parameters:
        final String[] workerLocationsStr = processWorkerLocations(arguments.get("-w"));
        final List<InetAddress> workerLocationsInet = convertFromStringToInetAddress(workerLocationsStr);
        workerLocationsInet.add(NetworkUtil.getLocalIPv4Address());

        final Integer replicationFactor = processInteger(arguments.get("-R"));

        final boolean startWorkersLocallyForTesting = processBoolean(arguments.get("-d"));

        throwExceptionIfNull(replicationFactor, "The replication factor of the database system must be specified with the -R argument.");

        String resultsFolderLocation = arguments.get("-l");

        if (resultsFolderLocation == null) {
            resultsFolderLocation = Coordinator.DEFAULT_RESULTS_FOLDER_LOCATION;
        }

        final List<String> script = createCoordinationScript(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, numberOfQueriesPerTransaction);
        ScriptRunner.runCoordinationScript("generatedDb", workerLocationsInet, replicationFactor, startWorkersLocallyForTesting, script, resultsFolderLocation, "generatedScript", 5);

        System.exit(0);
    }

    private static List<String> createCoordinationScript(final long runtime, final double probabilityOfFailure, final long frequencyOfFailure, final int numberOfMachines, final int numberOfTables, final int numberOfQueriesPerTransaction) throws IOException {

        final TableClustering clusteringSpec = new TableClustering(Clustering.GROUPED, 2);

        final Set<WorkloadType> workloadSpecs = new HashSet<WorkloadType>();
        final WorkloadType spec = new WorkloadType(0.5, false, numberOfQueriesPerTransaction, true, 5, LinkToTableLocation.GROUPED_WORKLOAD, true);
        workloadSpecs.add(spec);

        final List<String> coordinationScript = CoordinationScriptGenerator.generateCoordinationScript(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, clusteringSpec, workloadSpecs);
        return coordinationScript;
    }

    private static double processDouble(final String arg) {

        return arg == null ? 0 : Double.valueOf(arg);
    }

    private static long processLong(final String arg) {

        return arg == null ? 0 : Long.valueOf(arg);
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
