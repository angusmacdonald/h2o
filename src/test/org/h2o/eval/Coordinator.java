package org.h2o.eval;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.util.NetUtils;
import org.h2o.H2OLocator;
import org.h2o.eval.interfaces.ICoordinatorLocal;
import org.h2o.eval.interfaces.ICoordinatorRemote;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.eval.printing.AveragedResultsCSVPrinter;
import org.h2o.eval.printing.IndividualRunCSVPrinter;
import org.h2o.eval.script.coord.CoordinationScriptExecutor;
import org.h2o.eval.script.coord.instructions.CoordinatorScriptState;
import org.h2o.eval.script.coord.instructions.Instruction;
import org.h2o.eval.script.workload.Workload;
import org.h2o.eval.script.workload.WorkloadResult;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.JavaProcessDescriptor;
import uk.ac.standrews.cs.nds.util.CommandLineArgs;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.FileUtil;
import uk.ac.standrews.cs.nds.util.NetworkUtil;
import uk.ac.standrews.cs.nds.util.PrettyPrinter;

public class Coordinator implements ICoordinatorRemote, ICoordinatorLocal, ICoordinatorScript {

    /*
     * Registry fields.
     */
    private static final String REGISTRY_BIND_NAME = "h2oEvaluationCoordinator";
    private Registry registry;

    private final String databaseName;

    /*
     * Worker Fields
     */
    private final Set<InetAddress> workerLocations;
    private final List<IWorker> inactiveWorkers = new LinkedList<IWorker>();
    private final Set<IWorker> activeWorkers = new HashSet<IWorker>();

    /**
     * Mapping from an integer ID to worker node for H2O instances that have been started through
     * a co-ordination script.
     */

    private final Map<IWorker, Integer> workersWithActiveWorkloads = Collections.synchronizedMap(new HashMap<IWorker, Integer>());

    private CoordinatorScriptState coordScriptState = new CoordinatorScriptState(this);

    /*
     * Locator server fields.
     */
    private boolean locatorServerStarted = false;
    private H2OPropertiesWrapper descriptorFile;

    /*
     * Results file location:
     */
    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");
    public final static String DEFAULT_DATABASE_NAME = "MY_EVALUATION_DATABASE";
    public static final String DEFAULT_RESULTS_FOLDER_LOCATION = "generatedWorkloads";
    private String resultsFolderLocation;

    /*
     * Results log:
     */
    private final Date startDate = new Date();
    private final List<WorkloadResult> workloadResults = new LinkedList<WorkloadResult>();

    /**
     * The replication factor being used by the database (if set via the {@link #setReplicationFactor(int)} method - otherwise this will be 0).
     */
    private int replicationFactor;

    /**
     * Name of the configuration script being run by co-ordinator. This will be null if no co-ordinator scripts are being run.
     */
    private String coordinationScriptLocation;

    /**
     * 
     * @param databaseName Name of the evaluation database system this coordinator will create.
     * @param workerLocations IP addresses or hostnames of machines which are running worker nodes.
     */
    public Coordinator(final String databaseName, final InetAddress... workerLocations) {

        this(databaseName, Arrays.asList(workerLocations));
    }

    public Coordinator(final String databaseName, final String... workerLocationsStr) {

        this(databaseName, convertFromStringToInetAddress(workerLocationsStr));
    }

    public Coordinator(final String databaseName, final List<InetAddress> workerLocations) {

        this.databaseName = databaseName;
        this.workerLocations = new HashSet<InetAddress>();
        this.workerLocations.addAll(workerLocations);
        resultsFolderLocation = DEFAULT_DATABASE_NAME;

        bindToRegistry();
    }

    public void bindToRegistry() {

        try {
            registry = LocateRegistry.getRegistry();

            final Remote stub = UnicastRemoteObject.exportObject(this, 0);
            registry.bind(REGISTRY_BIND_NAME, stub);
        }
        catch (final Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * ICoordinatorLocal methods... 
     */
    @Override
    public int startH2OInstances(final int numberToStart) throws StartupException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "About to start H2O instance on " + numberToStart + " workers.");

        if (!locatorServerStarted) { throw new StartupException("The locator server has not yet been started."); }

        if (numberToStart <= 0) { return 0; }

        scanForWorkerNodes(workerLocations);

        if (inactiveWorkers.size() == 0) { return 0; }

        int numberStarted = 0;

        for (final IWorker worker : inactiveWorkers) {

            if (numberStarted >= numberToStart) {
                break;
            }

            try {
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting H2O instance on worker at " + worker.getHostname());
            }
            catch (final RemoteException e2) {
                //Doesn't matter because it's just a diagnostic. Handle the exception on a proper call to the worker.
            }

            try {
                worker.startH2OInstance(descriptorFile, false, false);

                activeWorkers.add(worker);

                numberStarted++;
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Incrementing number started: " + numberStarted);
            }
            catch (final Exception e) {
                try {
                    ErrorHandling.exceptionError(e, "Failed to start instance on worker at " + worker.getHostname());
                }
                catch (final RemoteException e1) {
                    ErrorHandling.exceptionError(e, "Failed to start instance on worker " + worker);
                }
            }
        }

        inactiveWorkers.removeAll(activeWorkers);

        return numberStarted;
    }

    /**
     * Start a single instance on a worker at the specified hostname.
     * @param hostname Where the H2O instance should be started.
     * @param createConnectionPropertiesFile Whether to create a benchmarkSQL properties file specifying how to connect to this instance.
     * @return the JDBC connections string for the recently started database, or null if it wasn't successfully started.
     * @throws StartupException
     */
    public String startH2OInstance(final InetAddress hostname, final boolean startInRemoteDebug, final boolean disableReplication) throws StartupException {

        if (!locatorServerStarted) { throw new StartupException("The locator server has not yet been started."); }

        scanForWorkerNodes(workerLocations);

        for (final IWorker worker : getAllWorkers()) {
            try {
                if (worker.getHostname().equals(hostname)) {

                    final String jdbcConnectionString = worker.startH2OInstance(descriptorFile, startInRemoteDebug, disableReplication);

                    swapWorkerToActiveSet(worker);

                    return jdbcConnectionString;

                }

            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to start instance " + worker);
            }
        }
        return null;
    }

    /**
     * Add a worker to the set of active workers, and removed it from the set of inactive workers.
     * @param worker
     */
    public void swapWorkerToActiveSet(final IWorker worker) {

        activeWorkers.add(worker);
        final boolean removed = inactiveWorkers.remove(worker);

        if (!removed) {
            ErrorHandling.errorNoEvent("Worker was not correctly removed from the list of active workers.");
        }
    }

    /**
     * Connect to all known workers and terminate any active H2O instances. Also remove the state of those instances.
     */
    public void obliterateExtantInstances() {

        scanForWorkerNodes(workerLocations);

        for (final IWorker worker : getAllWorkers()) {
            try {
                worker.terminateH2OInstance();
            }
            catch (final Exception e) {

            }
        }

        for (final IWorker worker : getAllWorkers()) {
            try {
                worker.deleteH2OInstanceState();
            }
            catch (final Exception e) {

            }
        }

    }

    public Set<IWorker> getAllWorkers() {

        final Set<IWorker> allWorkers = new HashSet<IWorker>();
        allWorkers.addAll(activeWorkers);
        allWorkers.addAll(inactiveWorkers);
        return allWorkers;
    }

    public IWorker startH2OInstance(final boolean disableReplication) throws StartupException, RemoteException {

        if (inactiveWorkers.size() == 0) {
            scanForWorkerNodes(workerLocations);

            if (inactiveWorkers.size() == 0) { throw new StartupException("No more workers are available. Could not instantiate another H2O instance."); }
        }

        final IWorker worker = inactiveWorkers.get(0);

        worker.startH2OInstance(descriptorFile, false, disableReplication);

        swapWorkerToActiveSet(worker);

        return worker;
    }

    /**
     * Go through the set of worker hosts and look for active worker instances in the registry on each host. In testing in particular there may
     * be multiple workers on each host.
     * @param workerLocations IP addresses or hostnames of machines which are running worker nodes. You can provide the {@link #workerLocations} field
     * as a parameter, or something else.
     */
    private void scanForWorkerNodes(final Set<InetAddress> workerLocations) {

        for (final InetAddress location : workerLocations) {

            try {
                final Registry remoteRegistry = LocateRegistry.getRegistry(location.getHostName());

                findActiveWorkersAtThisLocation(remoteRegistry);
            }
            catch (final Exception e) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to connect to a registry at '" + location + "'.");
            }

        }
    }

    /**
     * Find workers that exist at the registry located at the specified location.

     * @param remoteRegistry
     * @throws RemoteException
     * @throws AccessException
     */
    private void findActiveWorkersAtThisLocation(final Registry remoteRegistry) throws RemoteException, AccessException {

        final String[] remoteEntries = remoteRegistry.list();

        for (final String entry : remoteEntries) {
            if (entry.startsWith(Worker.REGISTRY_PREFIX)) {
                try {
                    final IWorker workerNode = (IWorker) remoteRegistry.lookup(entry);

                    if (workerNode != null && !activeWorkers.contains(workerNode) && !inactiveWorkers.contains(workerNode)) {
                        try {

                            workerNode.initiateConnection(NetUtils.getLocalAddress(), REGISTRY_BIND_NAME);

                            inactiveWorkers.add(workerNode);
                        }
                        catch (final RemoteException e) {
                            ErrorHandling.exceptionError(e, "Failed to connect to worker at " + workerNode + ".");
                        }
                    }
                }
                catch (final NotBoundException e) {
                    ErrorHandling.exceptionError(e, "Expected RMI registry entry '" + entry + "' was not bound.");
                }
            }
        }
    }

    /*
     * ICoordinatorRemote calls... 
     */

    @Override
    public synchronized void collateMonitoringResults(final WorkloadResult workloadResult) throws RemoteException {

        removeFromSetOfActiveWorkloads(workloadResult.getWorkloadID());
        System.out.println(workloadResult);

        workloadResults.add(workloadResult);

        if (workloadResult.getException() != null) {
            workloadResult.getException().printStackTrace();
        }
    }

    private synchronized void removeFromSetOfActiveWorkloads(final IWorker worker) {

        Integer count = workersWithActiveWorkloads.get(worker);
        if (count != null && count == 1) {
            workersWithActiveWorkloads.remove(worker);
        }
        else {
            workersWithActiveWorkloads.put(worker, --count);
        }

    }

    @Override
    public void startLocatorServer(final int locatorPort) throws IOException, StartupException {

        if (locatorServerStarted) { throw new StartupException("Locator server has already been started. It cannot be started twice."); }

        final List<String> args = getLocatorArgs(locatorPort);

        try {
            new HostDescriptor().getProcessManager().runProcess(new JavaProcessDescriptor().classToBeInvoked(H2OLocator.class).args(args));

        }
        catch (final Exception e) {
            e.printStackTrace();
            throw new StartupException("Failed to create new H2O locator process: " + e.getMessage());
        }

        try {
            Thread.sleep(3000); //wait for locator to start up.
        }
        catch (final InterruptedException e) {
        }

        final String descriptorFileLocation = Worker.PATH_TO_H2O_DATABASE + File.separator + databaseName + ".h2od";

        descriptorFile = H2OPropertiesWrapper.getWrapper(descriptorFileLocation);
        descriptorFile.loadProperties();
        locatorServerStarted = true;

    }

    public List<String> getLocatorArgs(final int locatorPort) {

        final List<String> args = new LinkedList<String>();

        args.add("-n" + databaseName);
        args.add("-p" + locatorPort);
        args.add("-dtrue");
        args.add("-f" + Worker.PATH_TO_H2O_DATABASE);
        args.add("-D6");
        return args;
    }

    @Override
    public void executeWorkload(final String workloadFileLocation) throws StartupException {

        final IWorker worker = getActiveWorker();

        executeWorkload(worker, workloadFileLocation, 0);

    }

    public void executeWorkload(final String id, final String workloadFileLocation, final long duration) throws StartupException {

        final IWorker worker = coordScriptState.getScriptedInstance(Integer.valueOf(id));

        if (worker == null) { throw new StartupException("No worker exists for this ID: " + id); }

        executeWorkload(worker, workloadFileLocation, duration);

    }

    private void executeWorkload(final IWorker worker, final String workloadFileLocation, final long duration) throws StartupException {

        Workload workload;
        try {
            workload = new Workload(workloadFileLocation, duration);
        }
        catch (final FileNotFoundException e) {
            ErrorHandling.exceptionError(e, "Couldn't find the workload file specified: " + workloadFileLocation);
            throw new StartupException("Couldn't find the workload file specified: " + workloadFileLocation);
        }
        catch (final IOException e) {
            ErrorHandling.exceptionError(e, "Couldn't read from the workload file specified: " + workloadFileLocation);
            throw new StartupException("Couldn't read from the workload file specified: " + workloadFileLocation);
        }

        try {
            worker.startWorkload(workload);
        }
        catch (final RemoteException e) {
            ErrorHandling.exceptionError(e, "Couldn't connect to remote worker instance.");
            throw new StartupException("Couldn't connect to remote worker instance.");
        }
        catch (final WorkloadParseException e) {
            ErrorHandling.exceptionError(e, "Error parsing workload in " + workloadFileLocation);
        }
        catch (final SQLException e) {
            ErrorHandling.exceptionError(e, "Error creating SQL statement for workload execution. Workload was never started.");
        }

        addWorkloadToRecords(worker);
    }

    private synchronized void addWorkloadToRecords(final IWorker worker) {

        if (workersWithActiveWorkloads.containsKey(worker)) {
            Integer currentCount = workersWithActiveWorkloads.get(worker);
            workersWithActiveWorkloads.put(worker, ++currentCount);
        }
        else {
            workersWithActiveWorkloads.put(worker, 1);
        }
    }

    /**
     * Get an active worker from the set of active workers.
     * @return null if there are no active workers.
     */
    private IWorker getActiveWorker() {

        for (final IWorker worker : activeWorkers) {
            return worker;
        }

        return null;
    }

    @Override
    public void blockUntilWorkloadsComplete() throws RemoteException {

        while (areThereActiveWorkloads()) {

            try {
                Thread.sleep(1000);
            }
            catch (final InterruptedException e) {
            }

        };

        coordScriptState.disableKillMonitor();

        try {
            IndividualRunCSVPrinter.printResults(resultsFolderLocation + File.separator + dateFormatter.format(startDate) + "-results.csv", workloadResults, coordScriptState.getFailureLog());
            AveragedResultsCSVPrinter.printResults(resultsFolderLocation + File.separator + "all.csv", workloadResults, coordinationScriptLocation, activeWorkers.size(), replicationFactor);
        }
        catch (final IOException e) {

            ErrorHandling.exceptionError(e, "Failed printing results file.");
        }

    }

    private synchronized boolean areThereActiveWorkloads() {

        return workersWithActiveWorkloads.size() > 0;
    }

    @Override
    public void executeCoordinatorScript(final String coordinatorScriptLocation) throws RemoteException, FileNotFoundException, WorkloadParseException, StartupException, SQLException, IOException, WorkloadException {

        executeCoordinatorScript(coordinatorScriptLocation, DEFAULT_RESULTS_FOLDER_LOCATION);
    }

    @Override
    public void executeCoordinatorScript(final String coordinationScriptLocation, final String resultsFolderLocation) throws FileNotFoundException, IOException, WorkloadParseException, StartupException, SQLException, WorkloadException {

        final List<String> script = FileUtil.readAllLines(coordinationScriptLocation);
        final List<Instruction> parsedInstructions = parseCoordinationScript(script);

        executeParsedCoordinationScript(parsedInstructions, resultsFolderLocation, coordinationScriptLocation);
    }

    private List<Instruction> parseCoordinationScript(final List<String> script) throws WorkloadParseException {

        final List<Instruction> instructions = new LinkedList<Instruction>();

        for (final String action : script) {
            final Instruction instruction = CoordinationScriptExecutor.parse(action);

            if (instruction != null) {
                instructions.add(instruction);
            }
        }

        return instructions;
    }

    @Override
    public void executeCoordinationScript(final List<String> script, final String resultsFolderLocation, final String coordinationScriptName) throws WorkloadParseException, RemoteException, StartupException, SQLException, WorkloadException {

        final List<Instruction> parsedInstructions = parseCoordinationScript(script);

        executeParsedCoordinationScript(parsedInstructions, resultsFolderLocation, coordinationScriptName);

    }

    private void executeParsedCoordinationScript(final List<Instruction> script, final String resultsFolderLocation, final String coordinationScriptLocation) throws WorkloadParseException, RemoteException, StartupException, SQLException, WorkloadException {

        this.resultsFolderLocation = resultsFolderLocation;
        this.coordinationScriptLocation = coordinationScriptLocation;

        coordScriptState = new CoordinatorScriptState(this);
        coordScriptState.startKillMonitor();

        for (final Instruction instruction : script) {
            instruction.execute(coordScriptState);
        }

    }

    public void executeQuery(final IWorker worker, final String query) throws RemoteException, SQLException {

        worker.executeQuery(query);
    }

    public void killInstance(final Integer workerID) throws RemoteException, ShutdownException {

        final IWorker worker = coordScriptState.getScriptedInstance(Integer.valueOf(workerID));

        worker.terminateH2OInstance();
    }

    /**
     * Start up a new set of H2O instances based on the provided parameters.
     * @param args
     *            <ul>
     *            <li><em>-n<name></em>. The name of the database system (i.e. the name of the database in the descriptor file, the global system).</li>
     *            <li><em>-w<name></em>. A list of all the locations where worker nodes may be running, delimited by a semi-colon (e.g. "hostname1;hostname2;hostname3"). It is assumed that
     *            an instance will be started on the local instance, so this hostname does not need to be included.</li>
     *            <li><em>-c<name></em>. The number of H2O instances that are to be started. This number must be less than or equal to the number of active worker instances.</li>
     *            <li><em>-t<name></em>. Optional. Whether to terminate any existing instances running at all workers (including stored state), before doing anything else.</li>
     *            <li><em>-p<name></em>. Optional. The path/name of the properties file to create stating how to connect to the system table (used by BenchmarkSQL)</li>
     *            <li><em>-r<name></em>. Optional. The system-wide replication factor for user tables.</li>
     *            <li><em>-d<name></em>. Optional (true/false). Whether to start the remote instance in Java remote debug mode.</li>
     *            </ul>
     * @throws StartupException Thrown if a required parameter was not specified.
     * @throws IOException 
     */
    public static void main(final String[] args) throws StartupException, IOException {

        Diagnostic.setLevel(DiagnosticLevel.FINAL);

        /*
         * Parse parameters.
         */
        final Map<String, String> arguments = CommandLineArgs.parseCommandLineArgs(args);

        final String databaseName = processDatabaseName(arguments.get("-n"));
        final String[] workerLocationsStr = processWorkerLocations(arguments.get("-w"));

        final List<InetAddress> workerLocationsInet = convertFromStringToInetAddress(workerLocationsStr);
        workerLocationsInet.add(NetworkUtil.getLocalIPv4Address());

        final int h2oInstancesToStart = processInteger(arguments.get("-c"));
        final int replicationFactor = processInteger(arguments.get("-r"));

        final boolean obliterateExistingInstances = processBoolean(arguments.get("-t"));
        final boolean startInRemoteDebug = processBoolean(arguments.get("-d"));

        final String connectionPropertiesFile = arguments.get("-p");

        /*
         * Create new Coordinator and start the specified number of database instances.
         */
        final Coordinator coord = new Coordinator(databaseName, workerLocationsInet);

        if (obliterateExistingInstances) {
            coord.obliterateExtantInstances();
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting locator server.");

        coord.startLocatorServer(34000);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Setting system-wide replication factor to " + replicationFactor);
        coord.setReplicationFactor(replicationFactor);

        /* 
         * Start first H2O instance.
         */
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting primary H2O instance on " + NetworkUtil.getLocalIPv4Address().getHostName());
        final InetAddress localhost = getLocalHostname();

        final String connectionString = coord.startH2OInstance(localhost, startInRemoteDebug, true); //start an instance locally as the system table.

        if (connectionString == null) { throw new StartupException("Failed to start the local H2O instance that is intended to become the System Table."); }

        /*
         * Start remaining H2O instances.
         */
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting secondary H2O instances on " + (h2oInstancesToStart - 1) + " of the following nodes: " + PrettyPrinter.toString(workerLocationsStr));
        final int started = coord.startH2OInstances(h2oInstancesToStart - 1);

        if (started != h2oInstancesToStart - 1) {
            System.err.println("Failed to start the correct number of instances. Started " + started + 1 + ", but needed to start " + h2oInstancesToStart + ".");
            System.exit(1); //do this rather than returning because extant processes spawned by this process will stop the JVM from terminating.
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Started " + h2oInstancesToStart + " H2O instances.");

        if (connectionPropertiesFile != null) {
            writeConnectionStringToPropertiesFile(connectionString, connectionPropertiesFile);
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "BenchmarkSQL connection information successfully written to '" + connectionPropertiesFile + "'.");
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Stopping co-ordinator.");

        System.exit(0);
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

    /* (non-Javadoc)
     * @see org.h2o.eval.interfaces.ICoordinatorLocal#setReplicationFactor(int)
     */
    @Override
    public void setReplicationFactor(final int replicationFactor) throws StartupException {

        this.replicationFactor = replicationFactor;

        if (descriptorFile == null) { throw new StartupException("Descriptor file has not been created yet. Call startLocatorServer() first."); }

        descriptorFile.setProperty("RELATION_REPLICATION_FACTOR", replicationFactor + "");
    }

    /* (non-Javadoc)
     * @see org.h2o.eval.interfaces.ICoordinatorLocal#setMetaDataReplicationFactor(int)
     */
    @Override
    public void setMetaDataReplicationFactor(final int replicationFactor) throws StartupException {

        if (descriptorFile == null) { throw new StartupException("Descriptor file has not been created yet. Call startLocatorServer() first."); }

        descriptorFile.setProperty("TABLE_MANAGER_REPLICATION_FACTOR", replicationFactor + "");
        descriptorFile.setProperty("SYSTEM_TABLE_REPLICATION_FACTOR", replicationFactor + "");
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

    /**
     * Writes the connection string to a properties file formatted for benchmarkSQL.
     * @param connectionString
     * @param propertiesFileLocation 
     * @throws IOException 
     */
    private static void writeConnectionStringToPropertiesFile(final String connectionString, final String propertiesFileLocation) throws IOException {

        final File f = new File(propertiesFileLocation);

        f.getParentFile().mkdirs();

        final StringBuilder prop = new StringBuilder();

        prop.append("name=H2O\n");
        prop.append("driver=org.h2.Driver\n");
        prop.append("conn=" + connectionString + "\n");
        prop.append("user=sa\n");
        prop.append("password=");

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Writing to properties file at: " + propertiesFileLocation);

        FileUtil.writeToFile(propertiesFileLocation, prop.toString());
    }

    private static boolean processBoolean(final String arg) {

        return arg == null ? false : Boolean.valueOf(arg);
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

        return arg == null ? DEFAULT_DATABASE_NAME : arg;
    }

    @Override
    public void shutdown() {

        coordScriptState.disableKillMonitor();

        for (final IWorker worker : coordScriptState.getScriptedInstanceValues()) {

            if (worker != null) {
                try {
                    worker.shutdownWorker();
                }
                catch (final RemoteException e) {
                    ErrorHandling.errorNoEvent("Failed to contact worker to shut it down.");
                }
            }
        }
    }

    public H2OPropertiesWrapper getDescriptorFile() {

        return descriptorFile;
    }

    @Override
    public void setupSystem(final int locatorServerPort, final int tableReplicationFactor, final int metadataReplicationFactor) throws IOException, StartupException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Deleting existing instances.");

        obliterateExtantInstances();

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting locator server on port " + locatorServerPort + ".");

        startLocatorServer(locatorServerPort);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Setting system-wide replication factor to " + replicationFactor);

        setReplicationFactor(tableReplicationFactor);
        setMetaDataReplicationFactor(metadataReplicationFactor);
    }

}
