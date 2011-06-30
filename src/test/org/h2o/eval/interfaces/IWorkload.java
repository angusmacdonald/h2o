package org.h2o.eval.interfaces;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2o.eval.Worker;
import org.h2o.eval.script.workload.WorkloadResult;

/**
 * Workload to be run on a worker node.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface IWorkload extends Serializable {

    /**
     * Set the {@link Connection} to be used for executing the workload and provide a reference to the worker instance which is managing this workloads execution.
     * @param connection Connection to be used to execute workload.
     * @param worker The worker managing the execution of this workload.
     */
    public void initialiseOnWorker(Connection connection, Worker worker);

    /**
     * Start executing the workload of this IWorkload instance, using the database connection given.
     * @param connection The database connection to be used when executing this workload.
     * @throws WorkloadParseException Thrown when the workload being executed contains a syntactic error.
     * @throws SQLException Thrown when an SQL statement cannot initially be created. Errors on individual queries while executing this workload do not throw exceptions, but log failure.
     */
    public WorkloadResult executeWorkload();

}
