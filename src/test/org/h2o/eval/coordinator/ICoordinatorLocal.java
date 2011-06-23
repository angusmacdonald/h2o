package org.h2o.eval.coordinator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

/**
 * Interface for issuing commands to the evaluation coordinator.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface ICoordinatorLocal {

    public void startLocatorServer(int locatorPort) throws IOException;

    /**
     * Start H2O instances at the specified locations.
     * @param numberToStart The number of H2O instances to start.
     * @return the number of instances that were successfully started.
     * @throws StartupException Thrown if the instances couldn't be started because the {@link #startLocatorServer(int)} method
     * has not yet been called.
     */
    public int startH2OInstances(int numberToStart) throws StartupException;

    /**
     * Initiate execution of a specified workload.
     * @param workloadFileLocation  The location of the file containing the workload to be executed.
     * @throws StartupException
     */
    public void executeWorkload(String workloadFileLocation) throws StartupException;

    /**
     * Execute a co-ordinator script on this co-ordinator.
     * @param configFileLocation Location of the co-ordinator script file.
     * @throws FileNotFoundException If the co-ordinator script file does not exist where specified.
     * @throws WorkloadParseException If the script file contains invalid syntax.
     * @throws StartupException If an H2O instance could not be started as requested.
     * @throws SQLException If a query could not be executed as requested.
     * @throws IOException If there was a problem accessing the co-ordinator script file.
     */
    public void executeCoordinatorScript(String configFileLocation) throws RemoteException, FileNotFoundException, WorkloadParseException, StartupException, SQLException, IOException;

    /**
     * Method returns when all workloads that have been started by this co-ordinator terminate (doesn't matter whether they terminate successfully or via an exception).
     * @throws RemoteException Thrown if, while checking whether workloads have finished on remote workers, there was a loss of communication to the remote worker.
     */
    public void blockUntilWorkloadsComplete() throws RemoteException;
}
