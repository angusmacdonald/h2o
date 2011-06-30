package org.h2o.eval.interfaces;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

/**
 * Worker classes register with a central co-ordinator (implementing {@link ICoordinatorRemote}) 
 * and accept requests to start/stop a local H2O instance, and to start/stop workloads which query these instances.
 * 
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface IWorker extends Remote {

    /**
     * Allows a remote {@link ICoordinatorRemote} to connect to the worker and provide a reference back to itself,
     * so that the results of workloads can be returned to the coordinator.
     * <p>This method can be called any number of times.
     * @param hostname The host on which the co-ordinator is running. The worker can find the co-ordinator through the registry on this instance.
     * @param bindName The name by which this co-ordinator is bound to its registry.
     * @throws NotBoundException If the co-ordinator could not be found at the specified registry.
     */
    public void initiateConnection(String hostname, String bindName) throws RemoteException, NotBoundException;

    /*
     * H2O Instance-Related Methods.
     */

    /**
     * Start a new H2O instance which will connect to the database system described by the database descriptor file provided.
     * <p>Blocks until the instance has started.
     * @param descriptorFileLocation Serialized version of the descriptor file.
     * @return true if an H2O instance was successfully started.
     * @throws StartupException If the instance couln't be started.
     */
    public void startH2OInstance(final H2OPropertiesWrapper descriptorFileLocation) throws RemoteException, StartupException;

    /**
     * Gracefully shutdown the H2O instance that this worker is running (by sending it a shutdown request).
     * @exception ShutdownException If the H2O instance couldn't be stopped, or if it wasn't running.
      */
    public void stopH2OInstance() throws RemoteException, ShutdownException;

    /**
     * Abruptly terminate the H2O instance that is running locally (by issuing a kill command on the process).
     * @exception ShutdownException If the H2O instance couldn't be stopped, or if it wasn't running.
    */
    public void terminateH2OInstance() throws RemoteException, ShutdownException;

    /**
     * Whether the worker is running an H2O instance locally.
     * @return true if the worker has started an instance, false if it hasn't.
     */
    public boolean isH2OInstanceRunning() throws RemoteException;

    /**
     * Delete any state that was created by H2O's instances previously run by this worker.
     * @throws ShutdownException If database state could not be deleted.
     */
    public void deleteH2OInstanceState() throws RemoteException, ShutdownException;

    /*
     * Workload-related methods.
     */

    /**
     * Start the given workload on this worker in a new thread by executing it against the local H2O instance.
     * @param workload Workload to be executed against the local H2O instance.
     * @return true if the workload was started successfully. <em>This method returns once the workload has started, not when it has finished.</em>
     * @throws WorkloadParseException Thrown when the workload being executed contains a syntactic error.
     * @throws SQLException Thrown when an SQL statement cannot initially be created. Errors on individual queries while executing this workload do not throw exceptions, but log failure.
     */
    public boolean startWorkload(IWorkload workload) throws RemoteException, WorkloadParseException, SQLException;

    /**
     * Stop the given workload from executing on this worker node.
     * @param workload The workload that should be stopped.
     * @return True if the workload was stopped, false it it couldn't be stopped or if it was not running.
     */
    public boolean stopWorkload(IWorkload workload) throws RemoteException;

    /**
     * Whether the specified workload is running on this worker.
     * @param workload The workload that this method tests for.
     * @return True if the workload is running, false if it isn't.
     */
    public boolean isWorkloadRunning(IWorkload workload) throws RemoteException;

    /**
     * Whether any workload is running on this worker.
     * @return True if a workload is running, false if none are running.
     */
    public boolean isWorkloadRunning() throws RemoteException;

    /**
     * Stop the checker thread which runs and checks whether any workloads have finished executing.
     */
    public void stopWorkloadChecker() throws RemoteException;

    /**
     * Execute a single SQL query on the worker's H2O instance.
     * @param query Query to be executed.
     * @throws SQLException If the query was not successfully executed.
     */
    public void executeQuery(String query) throws RemoteException, SQLException;

    /**
     * Get the local hostname on which this worker is running.
     * @return
     * @throws RemoteException
     */
    public String getHostname() throws RemoteException;
}
