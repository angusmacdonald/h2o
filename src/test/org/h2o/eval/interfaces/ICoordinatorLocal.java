package org.h2o.eval.interfaces;

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

    /**
     * 
     * @param locatorPort
     * @throws IOException
     * @throws StartupException If the locator server could not be started in a new process.
     */
    public void startLocatorServer(int locatorPort) throws IOException, StartupException;

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
     * @param block Whether to block waiting for the workload to complete.
     * @throws StartupException
     */
    public void executeWorkload(String workloadFileLocation, boolean block) throws StartupException;

    /**
     * Execute a co-ordinator script on this co-ordinator.
     * @param configFileLocation Location of the co-ordinator script file.
     * @throws FileNotFoundException If the co-ordinator script file does not exist where specified.
     * @throws WorkloadParseException If the script file contains invalid syntax.
     * @throws StartupException If an H2O instance could not be started as requested.
     * @throws SQLException If a query could not be executed as requested.
     * @throws IOException If there was a problem accessing the co-ordinator script file.
     * @throws WorkloadException If a workload operation (stall/resume) fails. See {@link IWorker#resumeWorkloads()} and {@link IWorker#stallWorkloads()}.
     */
    public void executeCoordinatorScript(String configFileLocation) throws RemoteException, FileNotFoundException, WorkloadParseException, StartupException, SQLException, IOException, WorkloadException;

    /**
     * Method returns when all workloads that have been started by this co-ordinator terminate (doesn't matter whether they terminate successfully or via an exception).
     * @throws RemoteException Thrown if, while checking whether workloads have finished on remote workers, there was a loss of communication to the remote worker.
     */
    public void blockUntilWorkloadsComplete() throws RemoteException;

    /**
     * Sets the desired replication factor in the database descriptor file. This will only be used if it is set before a database is started up.
     * @param replicationFactor How many copies of each table the system should aim to create.
     * @throws StartupException If the descriptor file hasn't been created yet.
     */
    public abstract void setReplicationFactor(final int replicationFactor) throws StartupException;

    /**
     * Set the replication factor for meta-data replication (of both the System Table and Table Manager).
     * @param replicationFactor How many copies of each meta-data table the system should aim to create.
     * @throws StartupException If the descriptor file hasn't been created yet.
     */
    public abstract void setMetaDataReplicationFactor(final int replicationFactor) throws StartupException;

    /**
     * Shutdown this co-ordinator by killing any extant threads.
     */
    public void shutdown();
}
