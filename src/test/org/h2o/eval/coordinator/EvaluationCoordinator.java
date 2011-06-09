package org.h2o.eval.coordinator;

import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import org.h2o.H2OLocator;
import org.h2o.eval.interfaces.ICoordinatorRemote;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;
import org.h2o.eval.worker.EvaluationWorker;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;

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

            registry.bind(REGISTRY_BIND_NAME, UnicastRemoteObject.exportObject(registry, 0));
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
     * Go through the set of worker locations and look for active worker instances.
     * @param pWorkerLocations IP addresses or hostnames of machines which are running worker nodes. You can provide the {@link #workerLocations} field
     * as a parameter, or something else.
     */
    public void scanForWorkerNodes(final String[] pWorkerLocations) {

        for (final String location : pWorkerLocations) {

            try {
                final Registry remoteRegistry = LocateRegistry.getRegistry(location);

                final String[] remoteEntries = remoteRegistry.list();

                findActiveWorkersAtThisLocation(location, remoteRegistry, remoteEntries);
            }
            catch (final Exception e) {
                ErrorHandling.exceptionError(e, "Failed to connect to a registry at '" + location + "'.");
            }

        }
    }

    public void findActiveWorkersAtThisLocation(final String location, final Registry remoteRegistry, final String[] remoteEntries) throws RemoteException, AccessException {

        for (final String entry : remoteEntries) {
            if (entry.startsWith(EvaluationWorker.REGISTRY_PREFIX)) {
                try {
                    final IWorker workerNode = (IWorker) remoteRegistry.lookup(entry);

                    if (workerNode != null) {
                        try {

                            workerNode.initiateConnection(null);

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
    public void collateMonitoringResults(final IWorkload workload, final long workloadLength, final long numAttemptedTransactions, final long numSuccessfulTransactions, final String[] eventHistory) throws RemoteException {

        // TODO Auto-generated method stub

    }

    @Override
    public void startLocatorServer(final int locatorPort) throws IOException {

        locatorServer = new H2OLocator(databaseName, locatorPort, true, EvaluationWorker.PATH_TO_H2O_DATABASE);
        final String descriptorFileLocation = locatorServer.start();

        descriptorFile = H2OPropertiesWrapper.getWrapper(descriptorFileLocation);
        descriptorFile.loadProperties();

        locatorServerStarted = true;

    }

}
