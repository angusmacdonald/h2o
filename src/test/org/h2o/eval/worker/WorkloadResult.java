package org.h2o.eval.worker;

import java.io.Serializable;

import org.h2o.eval.interfaces.IWorker;

public class WorkloadResult implements Serializable {

    private static final long serialVersionUID = -5199895309642140637L;

    /**
     * Whether the workload was successfully started.
     */
    private final boolean successfullyStarted;

    /**
     * Will be null if {@link #successfullyStarted} is true. Otherwise, either an SQLException, thrown because a database connection 
     * could not be created, or a WorkloadParseException, because the workload script is not syntactically correct.
     */
    private final Exception exception;

    /**
     * The number of transactions in this workload that were successfully executed.
     */
    private long successfullyExecutedTransactions;

    /**
     * The number of transactions in this workload that were attempted (includes both successful and unsuccessful executions).
     */
    private long totalAttemptedTransactions;

    private IWorker worker;

    /**
     * Called when a workload has not successfully initiated.
     * @param e Either an SQLException, thrown because a database connection could not be created, or a WorkloadParseException, because the workload script
     * is not syntactically correct.
     */
    public WorkloadResult(final Exception e) {

        successfullyStarted = false;
        exception = e;
    }

    public WorkloadResult(final long successfullyExecutedTransactions, final long totalAttemptedTransactions, final IWorker worker) {

        successfullyStarted = true;
        exception = null;

        this.successfullyExecutedTransactions = successfullyExecutedTransactions;
        this.totalAttemptedTransactions = totalAttemptedTransactions;
        this.worker = worker;
    }

    @Override
    public String toString() {

        if (successfullyStarted) {
            return "WorkloadResult [successfullyExecutedTransactions=" + successfullyExecutedTransactions + ", totalAttemptedTransactions=" + totalAttemptedTransactions + "]";
        }
        else {
            return "WorkloadResult [exception=" + exception + "]";
        }
    }

    public Object getWorkloadID() {

        return worker;
    }

    public Throwable getException() {

        return exception;
    }

}
