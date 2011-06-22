package org.h2o.eval.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.h2o.eval.worker.WorkloadResult;

/**
 * Implemented by the class co-ordinating H2O evaluations -- the class that initiates evaluations, sends out requests to run/stop H2O instances and run/stop benchmarks on worker nodes.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface ICoordinatorRemote extends Remote {

    /**
     * Accept monitoring results from worker nodes.
     * @param workloadResult The result of executing this workload.
     * @throws RemoteException
     */

    public void collateMonitoringResults(WorkloadResult workloadResult) throws RemoteException;
}
