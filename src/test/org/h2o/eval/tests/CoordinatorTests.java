package org.h2o.eval.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

import org.h2.tools.DeleteDbFiles;
import org.h2o.eval.Coordinator;
import org.h2o.eval.Worker;
import org.h2o.eval.interfaces.ICoordinatorLocal;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.script.coord.CoordinationScriptGenerator;
import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableClustering.Clustering;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.coord.specification.WorkloadType.LinkToTableLocation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Tests of the H2O co-ordinator class, {@link Coordinator}.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class CoordinatorTests {

    private IWorker[] workers = null;

    @Before
    public void setUp() throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);

        DeleteDbFiles.execute(Worker.PATH_TO_H2O_DATABASE, null, true);
    }

    @After
    public void tearDown() throws Exception {

        DeleteDbFiles.execute(Worker.PATH_TO_H2O_DATABASE, null, true);

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
        workers[0] = new Worker();

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        assertEquals(1, eval.startH2OInstances(1));
    }

    @Test
    public void startTwoWorkers() throws Exception {

        workers = new IWorker[2];
        workers[0] = new Worker();
        workers[1] = new Worker();

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        assertEquals(2, eval.startH2OInstances(2));
    }

    @Test
    public void startThreeWorkers() throws Exception {

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        assertEquals(3, eval.startH2OInstances(3));
    }

    @Test
    public void runWorkloadOnWorker() throws Exception {

        workers = new IWorker[1];
        workers[0] = new Worker();

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        assertEquals(1, eval.startH2OInstances(1));

        eval.executeWorkload("src/test/org/h2o/eval/workloads/test.workload", false);

        eval.blockUntilWorkloadsComplete();
    }

    @Test
    public void runCoordinationScript() throws Exception {

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/test.coord");

        eval.blockUntilWorkloadsComplete();
    }

    @Test
    public void runStaticGeneratedCoordinationScript() throws Exception {

        workers = new IWorker[2];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/coordinator.coord");

        eval.blockUntilWorkloadsComplete();
    }

    @Test
    public void runMigrateCoordinationScript() throws Exception {

        workers = new IWorker[2];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/st_failure/single-failure.coord");

        eval.blockUntilWorkloadsComplete();
    }

    @Test
    public void generateThenRunCoordinationScript() throws Exception {

        final String coordScriptLocation = generateCoordinationScript(false, 0);

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript(coordScriptLocation);

        eval.blockUntilWorkloadsComplete();
    }

    @Test
    public void generateThenRunSystemTableCoordinationScript() throws Exception {

        final String coordScriptLocation = generateCoordinationScript(true, 200);

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "127.0.0.1");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript(coordScriptLocation);

        eval.blockUntilWorkloadsComplete();
    }

    public String generateCoordinationScript(final boolean againstSystemTable, final int sleepTime) throws IOException {

        final long runtime = 60000;
        final double probabilityOfFailure = 0.5;
        final long frequencyOfFailure = 30000;
        final int numberOfMachines = 3;
        final int numberOfTables = 1;
        final TableClustering clusteringSpec = new TableClustering(Clustering.GROUPED, 5);

        final Set<WorkloadType> workloadSpecs = new HashSet<WorkloadType>();
        final WorkloadType spec = new WorkloadType(0.5, againstSystemTable, sleepTime, true, 120, LinkToTableLocation.WORKLOAD_PER_TABLE, false);
        workloadSpecs.add(spec);

        return CoordinationScriptGenerator.generateCoordinationScriptAndWriteToFile(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, clusteringSpec, workloadSpecs);
    }
}
