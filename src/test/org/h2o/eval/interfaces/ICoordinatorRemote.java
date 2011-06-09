package org.h2o.eval.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Implemented by the class co-ordinating H2O evaluations -- the class that initiates evaluations, sends out requests to run/stop H2O instances and run/stop benchmarks on worker nodes.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface ICoordinatorRemote extends Remote {

    /**
     * Accept monitoring results from worker nodes.
     * @param workload The workload that was executed.
     * @param workloadLength The time taken to execute this workload.
     * @param numAttemptedTransactions The number of transactions that the workload attempted to execute (includes both successes and failures).
     * @param numSuccessfulTransactions The number of transactions that successfully committed as part of this workload.
     * @param eventHistory Detailed data on the workload's execution, including the timing of specific transactions. 
     * @throws RemoteException
     */
    public void collateMonitoringResults(IWorkload workload, long workloadLength, long numAttemptedTransactions, long numSuccessfulTransactions, String[] eventHistory) throws RemoteException;
}
