package org.h2o.eval.interfaces;

import java.rmi.RemoteException;

import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;

/**
 * Worker classes register with a central co-ordinator (implementing {@link ICoordinator}) 
 * and accept requests to start/stop a local H2O instance, and to start/stop workloads which query these instances.
 * 
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface IWorker {

    /*
     * H2O Instance-Related Methods.
     */

    /**
     * Start a new H2O instance which will connect to the database system described by the database descriptor file provided.
     * @param descriptorFileLocation Location of the database descriptor file (can be a URI or a local file system path).
     * @return true if an H2O instance was successfully started.
     * @throws StartupException If the instance couln't be started.
     */
    public void startH2OInstance(final String descriptorFileLocation) throws RemoteException, StartupException;

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
     */
    public void deleteH2OInstanceState() throws RemoteException;

    /*
     * Workload-related methods.
     */

    /**
     * Start the given workload on this worker by executing it against the local H2O instance.
     * @param workload Workload to be executed against the local H2O instance.
     * @return true if the workload was started successfully.
     */
    public boolean startWorkload(IWorkload workload) throws RemoteException;

    /**
     * Stop the given workload from executing on this worker node.
     * @param workload The workload that should be stopped.
     * @return True if the workload was stopped, false it it couldn't be stopped or if it was not running.
     */
    public boolean stopWorkload(IWorkload workload) throws RemoteException;

    /**
     * Whether the specified workload is running on this worker.
     * @param workload The workload that this method tests for.
     * @return True if the workload is running, false it it isn't.
     */
    public boolean isWorkloadRunning(IWorkload workload) throws RemoteException;
}
