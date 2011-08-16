package org.h2o.eval.script.workload;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

import org.h2o.eval.Worker;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;
import org.h2o.eval.interfaces.WorkloadException;

import uk.ac.standrews.cs.nds.util.FileUtil;

public class Workload extends Thread implements IWorkload {

    private static final long serialVersionUID = -4540759495085136103L;

    /**
     * Queries to be executed as part of this workload.
     */
    private final ArrayList<String> queries;

    private Connection connection;

    private IWorker worker;

    /**
     * Duration for which the workload should be executed.
     */
    private final long duration;

    private WorkloadExecutor workloadExecutor;

    public Workload(final String workloadFileLocation, final long duration) throws FileNotFoundException, IOException {

        this.duration = duration;

        queries = FileUtil.readAllLines(workloadFileLocation);

    }

    @Override
    public void initialiseOnWorker(final Connection connection, final Worker worker) {

        this.connection = connection;
        this.worker = worker;

    }

    @Override
    public WorkloadResult executeWorkload() {

        try {
            workloadExecutor = new WorkloadExecutor();
            return workloadExecutor.execute(connection, queries, worker, duration, this);
        }
        catch (final Exception e) {
            e.printStackTrace();
            return new WorkloadResult(e, worker, this);
        }

    }

    @Override
    public void stallWorkload() throws WorkloadException {

        workloadExecutor.stall();
    }

    @Override
    public void resumeWorkload() throws WorkloadException {

        workloadExecutor.resume();

    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (queries == null ? 0 : queries.hashCode());
        result = prime * result + (worker == null ? 0 : worker.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final Workload other = (Workload) obj;
        if (queries == null) {
            if (other.queries != null) { return false; }
        }
        else if (!queries.equals(other.queries)) { return false; }
        if (worker == null) {
            if (other.worker != null) { return false; }
        }
        else if (!worker.equals(other.worker)) { return false; }
        return true;
    }
}
