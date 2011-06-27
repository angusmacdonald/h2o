package org.h2o.eval.coordinator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.h2o.eval.interfaces.ICoordinatorRemote;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.printing.CSVPrinter;
import org.h2o.eval.script.coord.CoordinationScriptExecutor;
import org.h2o.eval.script.coord.instructions.Instruction;
import org.h2o.eval.script.coord.instructions.MachineInstruction;
import org.h2o.eval.script.coord.instructions.WorkloadInstruction;
import org.h2o.eval.script.workload.Workload;
import org.h2o.eval.script.workload.WorkloadResult;
import org.h2o.eval.worker.EvaluationWorker;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.nds.util.FileUtil;

public class EvaluationCoordinator implements ICoordinatorRemote, ICoordinatorLocal {

    /*
     * Registry fields.
     */
    private static final String REGISTRY_BIND_NAME = "h2oEvaluationCoordinator";
    private Registry registry;

    private final String databaseName;

    /*
     * Worker Fields
     */
    private final String[] workerLocations;
    private final List<IWorker> inactiveWorkers = new LinkedList<IWorker>();
    private final Set<IWorker> activeWorkers = new HashSet<IWorker>();

    /**
     * Mapping from an integer ID to worker node for H2O instances that have been started through
     * a co-ordination script.
     */
    private final Map<Integer, IWorker> scriptedInstances = new HashMap<Integer, IWorker>();

    private final Set<IWorker> workersWithActiveWorkloads = Collections.synchronizedSet(new HashSet<IWorker>());

    /*
     * Locator server fields.
     */
    private boolean locatorServerStarted = false;
    private H2OLocator locatorServer;
    private H2OPropertiesWrapper descriptorFile;
    private KillMonitorThread killMonitor;

    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");

    private final Date startDate = new Date();

    /**
     * 
     * @param databaseName Name of the evaluation database system this coordinator will create.
     * @param workerLocations IP addresses or hostnames of machines which are running worker nodes.
     */
    public EvaluationCoordinator(final String databaseName, final String... workerLocations) {

        this.databaseName = databaseName;
        this.workerLocations = workerLocations;
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

        //TODO this mechanism for starting instances does not assign IDs to instances.

        if (!locatorServerStarted) { throw new StartupException("The locator server has not yet been started."); }

        if (numberToStart <= 0) { return 0; }

        scanForWorkerNodes(workerLocations);

        if (inactiveWorkers.size() == 0) { return 0; }

        int numberStarted = 0;

        for (final IWorker worker : inactiveWorkers) {
            try {
                worker.startH2OInstance(descriptorFile);

                activeWorkers.add(worker);

                numberStarted++;
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to start instance " + worker);
            }
        }

        inactiveWorkers.removeAll(activeWorkers);

        return numberStarted;
    }

    private IWorker startH2OInstance() throws StartupException, RemoteException {

        if (inactiveWorkers.size() == 0) {
            scanForWorkerNodes(workerLocations);

            if (inactiveWorkers.size() == 0) { throw new StartupException("Could not instantiated another H2O instance."); }
        }

        final IWorker worker = inactiveWorkers.get(0);

        worker.startH2OInstance(descriptorFile);

        inactiveWorkers.remove(worker);
        activeWorkers.add(worker);

        return worker;
    }

    /**
     * Go through the set of worker hosts and look for active worker instances in the registry on each host. In testing in particular there may
     * be multiple workers on each host.
     * @param pWorkerLocations IP addresses or hostnames of machines which are running worker nodes. You can provide the {@link #workerLocations} field
     * as a parameter, or something else.
     */
    private void scanForWorkerNodes(final String[] pWorkerLocations) {

        for (final String location : pWorkerLocations) {

            try {
                final Registry remoteRegistry = LocateRegistry.getRegistry(location);

                findActiveWorkersAtThisLocation(remoteRegistry);
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to connect to a registry at '" + location + "'.");
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
            if (entry.startsWith(EvaluationWorker.REGISTRY_PREFIX)) {
                try {
                    final IWorker workerNode = (IWorker) remoteRegistry.lookup(entry);

                    if (workerNode != null && !activeWorkers.contains(workerNode)) {
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
    public void collateMonitoringResults(final WorkloadResult workloadResult) throws RemoteException {

        workersWithActiveWorkloads.remove(workloadResult.getWorkloadID());
        System.out.println(workloadResult);

        try {
            CSVPrinter.printResults("generatedWorkloads" + File.separator + dateFormatter.format(startDate) + "-results.csv", workloadResult);
        }
        catch (final FileNotFoundException e) {
            ErrorHandling.exceptionError(e, "Failed to create file to save results to.");
        }

        if (workloadResult.getException() != null) {
            workloadResult.getException().printStackTrace();
        }
    }

    @Override
    public void startLocatorServer(final int locatorPort) throws IOException {

        locatorServer = new H2OLocator(databaseName, locatorPort, true, EvaluationWorker.PATH_TO_H2O_DATABASE);
        final String descriptorFileLocation = locatorServer.start();

        descriptorFile = H2OPropertiesWrapper.getWrapper(descriptorFileLocation);
        descriptorFile.loadProperties();

        locatorServerStarted = true;

    }

    @Override
    public void executeWorkload(final String workloadFileLocation) throws StartupException {

        final IWorker worker = getActiveWorker();

        executeWorkload(worker, workloadFileLocation, 0);

    }

    private void executeWorkload(final String id, final String workloadFileLocation, final long duration) throws StartupException {

        final IWorker worker = scriptedInstances.get(Integer.valueOf(id));

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

        workersWithActiveWorkloads.add(worker);
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

        while (workersWithActiveWorkloads.size() > 0) {

            try {
                Thread.sleep(1000);
            }
            catch (final InterruptedException e) {
            }

        };

        if (killMonitor != null) {
            killMonitor.setRunning(false);
        }
    }

    @Override
    public void executeCoordinatorScript(final String configFileLocation) throws RemoteException, FileNotFoundException, WorkloadParseException, StartupException, SQLException, IOException {

        final List<String> script = FileUtil.readAllLines(configFileLocation);

        killMonitor = new KillMonitorThread(this);

        if (killMonitor.isRunning()) {
            killMonitor.setRunning(false);
            killMonitor = new KillMonitorThread(this);
        }

        killMonitor.start();

        for (final String action : script) {
            if (action.startsWith("{start_machine")) {

                final MachineInstruction startInstruction = CoordinationScriptExecutor.parseStartMachine(action);

                final IWorker worker = startH2OInstance();

                scriptedInstances.put(startInstruction.id, worker);
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Starting machine with ID '" + startInstruction.id + "'");

                if (startInstruction.fail_after != null) {
                    killMonitor.addKillOrder(startInstruction.id, System.currentTimeMillis() + startInstruction.fail_after);
                }
            }
            else if (action.startsWith("{terminate_machine")) {

                final MachineInstruction terminateInstruction = CoordinationScriptExecutor.parseTerminateMachine(action);

                try {
                    killInstance(terminateInstruction.id);
                }
                catch (final ShutdownException e) {
                    ErrorHandling.exceptionError(e, "Failed to shutdown instance with ID " + terminateInstruction.id + ".");
                }
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Terminated machine with ID '" + terminateInstruction.id + "'");

            }
            else if (action.startsWith("{sleep=")) {
                final int sleepTime = CoordinationScriptExecutor.parseSleepOperation(action);

                try {
                    Thread.sleep(sleepTime);
                }
                catch (final InterruptedException e) {
                }

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Sleeping for '" + sleepTime + "'");

            }
            else {
                final Instruction instruction = CoordinationScriptExecutor.parseQuery(action);

                if (instruction.isWorkload()) {
                    final WorkloadInstruction wi = (WorkloadInstruction) instruction;
                    executeWorkload(wi.id, wi.workloadFile, wi.duration);

                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing workload '" + wi.workloadFile + "' for '" + wi.duration + "', on '" + wi.id + "'.");

                }
                else {
                    executeQuery(instruction.id, instruction.getData());

                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing query '" + instruction.getData() + "' on '" + instruction.id + "'.");

                }
            }
        }
    }

    public void executeCoordinationScript(final List<String> script) throws WorkloadParseException, RemoteException, StartupException, SQLException {

    }

    private void executeQuery(final String workerID, final String query) throws RemoteException, SQLException {

        final IWorker worker = scriptedInstances.get(Integer.valueOf(workerID));

        worker.executeQuery(query);
    }

    public void killInstance(final Integer workerID) throws RemoteException, ShutdownException {

        final IWorker worker = scriptedInstances.get(Integer.valueOf(workerID));

        worker.terminateH2OInstance();
    }

}
