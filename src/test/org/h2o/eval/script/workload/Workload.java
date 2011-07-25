package org.h2o.eval.script.workload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

import org.h2o.eval.Worker;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;

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

    public Workload(final String workloadFileLocation, final long duration) throws FileNotFoundException, IOException {

        this.duration = duration;
        final File workloadFile = new File(workloadFileLocation);

        if (!workloadFile.exists()) { throw new FileNotFoundException("Workload file doesn't exist."); }

        queries = FileUtil.readAllLines(workloadFile);

    }

    @Override
    public void initialiseOnWorker(final Connection connection, final Worker worker) {

        this.connection = connection;
        this.worker = worker;

    }

    @Override
    public WorkloadResult executeWorkload() {

        try {
            return WorkloadExecutor.execute(connection, queries, worker, duration);
        }
        catch (final Exception e) {
            e.printStackTrace();
            return new WorkloadResult(e, worker);
        }

    }
}
