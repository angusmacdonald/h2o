package org.h2o.eval.workload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;
import org.h2o.eval.worker.EvaluationWorker;

import uk.ac.standrews.cs.nds.util.FileUtil;

public class Workload extends Thread implements IWorkload {

    private static final long serialVersionUID = -4540759495085136103L;

    /**
     * Queries to be executed as part of this workload.
     */
    private final ArrayList<String> queries;

    private Connection connection;

    private IWorker worker;

    public Workload(final String workloadFileLocation) throws FileNotFoundException, IOException {

        final File workloadFile = new File(workloadFileLocation);

        if (!workloadFile.exists()) { throw new FileNotFoundException("Workload file doesn't exist."); }

        queries = FileUtil.readAllLines(workloadFile);

    }

    @Override
    public void initialiseOnWorker(final Connection connection, final EvaluationWorker worker) {

        this.connection = connection;
        this.worker = worker;

    }

    @Override
    public WorkloadResult executeWorkload() {

        try {
            return WorkloadExecutor.execute(connection, queries, worker);
        }
        catch (final Exception e) {
            return new WorkloadResult(e);
        }

    }
}
