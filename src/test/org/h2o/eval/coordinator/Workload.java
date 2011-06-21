package org.h2o.eval.coordinator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2o.eval.interfaces.IWorkload;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.util.FileUtil;

public class Workload implements IWorkload {

    private static final long serialVersionUID = -4540759495085136103L;

    /**
     * Queries to be executed as part of this workload.
     */
    private final ArrayList<String> queries;

    public Workload(final String workloadFileLocation) throws FileNotFoundException, IOException {

        final File workloadFile = new File(workloadFileLocation);

        if (!workloadFile.exists()) { throw new FileNotFoundException("Workload file doesn't exist."); }

        queries = FileUtil.readAllLines(workloadFile);

    }

    @Override
    public void execute(final Connection connection) throws WorkloadParseException, SQLException {

        WorkloadExecutor.execute(connection, queries);
    }
}
