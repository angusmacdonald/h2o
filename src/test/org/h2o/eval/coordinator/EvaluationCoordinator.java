package org.h2o.eval.coordinator;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.h2.util.NetUtils;
import org.h2o.H2OLocator;
import org.h2o.eval.interfaces.ICoordinatorRemote;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.worker.EvaluationWorker;
import org.h2o.eval.worker.WorkloadResult;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

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
    private final Set<IWorker> workerNodes = new HashSet<IWorker>();
    private final Set<IWorker> activeWorkers = new HashSet<IWorker>();

    private final Set<IWorker> workersWithActiveWorkloads = Collections.synchronizedSet(new HashSet<IWorker>());

    /*
     * Locator server fields.
     */
    private boolean locatorServerStarted = false;
    private H2OLocator locatorServer;
    private H2OPropertiesWrapper descriptorFile;

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

        if (!locatorServerStarted) { throw new StartupException("The locator server has not yet been started."); }

        if (numberToStart <= 0) { return 0; }

        scanForWorkerNodes(workerLocations);

        if (workerNodes.size() == 0) { return 0; }

        int numberStarted = 0;

        for (final IWorker worker : workerNodes) {
            try {
                worker.startH2OInstance(descriptorFile);

                activeWorkers.add(worker);

                numberStarted++;
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to start instance " + worker);
            }
        }

        return numberStarted;
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

                    if (workerNode != null) {
                        try {

                            workerNode.initiateConnection(NetUtils.getLocalAddress(), REGISTRY_BIND_NAME);

                            workerNodes.add(workerNode);
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

        Workload workload;
        try {
            workload = new Workload(workloadFileLocation);
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
    }

}
