package org.h2o.eval.script.workload;

import java.io.Serializable;
import java.util.List;

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

    private final List<QueryLogEntry> queryLog;

    /**
     * Called when a workload has not successfully initiated.
     * @param e Either an SQLException, thrown because a database connection could not be created, or a WorkloadParseException, because the workload script
     * is not syntactically correct.
     */
    public WorkloadResult(final Exception e) {

        successfullyStarted = false;
        exception = e;
        queryLog = null;
    }

    public WorkloadResult(final List<QueryLogEntry> queryLog, final long successfullyExecutedTransactions, final long totalAttemptedTransactions, final IWorker worker) {

        successfullyStarted = true;
        exception = null;

        this.successfullyExecutedTransactions = successfullyExecutedTransactions;
        this.totalAttemptedTransactions = totalAttemptedTransactions;
        this.worker = worker;
        this.queryLog = queryLog;
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

    public List<QueryLogEntry> getQueryLog() {

        return queryLog;
    }

}
