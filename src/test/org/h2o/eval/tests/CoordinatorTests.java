package org.h2o.eval.tests;

import static org.junit.Assert.assertEquals;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.h2.tools.DeleteDbFiles;
import org.h2o.eval.coordinator.EvaluationCoordinator;
import org.h2o.eval.coordinator.ICoordinatorLocal;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.worker.EvaluationWorker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Tests of the H2O co-ordinator class, {@link EvaluationCoordinator}.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class CoordinatorTests {

    private IWorker[] workers = null;

    @Before
    public void setUp() throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);

        DeleteDbFiles.execute(EvaluationWorker.PATH_TO_H2O_DATABASE, null, true);
    }

    @After
    public void tearDown() throws Exception {

        DeleteDbFiles.execute(EvaluationWorker.PATH_TO_H2O_DATABASE, null, true);

        for (final IWorker worker : workers) {

            if (worker != null) {
                try {
                    worker.terminateH2OInstance();
                }
                catch (final Exception e) { //Will happen if an H2O instance isn't running.
                }
            }
        }

        final Registry reg = LocateRegistry.getRegistry();

        for (final String name : reg.list()) {
            reg.unbind(name);
        }

    }

    @Test
    public void startOneWorker() throws Exception {

        workers = new IWorker[1];
        workers[0] = new EvaluationWorker();

        final ICoordinatorLocal eval = new EvaluationCoordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        assertEquals(1, eval.startH2OInstances(1));
    }

    @Test
    public void startTwoWorkers() throws Exception {

        workers = new IWorker[2];
        workers[0] = new EvaluationWorker();
        workers[1] = new EvaluationWorker();

        final ICoordinatorLocal eval = new EvaluationCoordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        assertEquals(2, eval.startH2OInstances(2));
    }

    @Test
    public void startThreeWorkers() throws Exception {

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new EvaluationWorker();
        }

        final ICoordinatorLocal eval = new EvaluationCoordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        assertEquals(3, eval.startH2OInstances(3));
    }

    @Test
    public void runWorkloadOnWorker() throws Exception {

        workers = new IWorker[1];
        workers[0] = new EvaluationWorker();

        final ICoordinatorLocal eval = new EvaluationCoordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        assertEquals(1, eval.startH2OInstances(1));

        eval.executeWorkload("src/test/org/h2o/eval/workloads/test.workload");

        eval.blockUntilWorkloadsComplete();
    }
}
