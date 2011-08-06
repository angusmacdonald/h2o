package org.h2o.eval.tests;

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
 * Tests of the co-ordinator scripts run through the {@link Coordinator} class.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class ScriptTests {

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
                    worker.shutdownWorker();
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

    /**
     * The instance holding the table manager fails, but queries are still answered by a second instance when the TM is recreated.
     */
    @Test
    public void singleFailureScript() throws Exception {

        workers = new IWorker[5];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/failure/coordinator-singlefailure.coord");

        eval.blockUntilWorkloadsComplete();

        eval.shutdown();
    }

    /**
     * One TM fails, it is recovered, but fails on a second machine... all subsequent queries fail.
     * @throws Exception
     */
    @Test
    public void doubleFailureScript() throws Exception {

        workers = new IWorker[5];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/failure/coordinator-doublefailure-threemachines.coord");

        eval.blockUntilWorkloadsComplete();

        eval.shutdown();
    }

    /**
     * One TM fails, it is recovered, but fails on a second machine... it is recreated again and runs successfully on a third machine.
     * @throws Exception
     */
    @Test
    public void doubleFailureFourMachines() throws Exception {

        workers = new IWorker[5];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript("src/test/org/h2o/eval/workloads/failure/coordinator-doublefailure-fourmachines.coord");

        eval.blockUntilWorkloadsComplete();

        eval.shutdown();
    }

    @Test
    public void generateThenRunCoordinationScript() throws Exception {

        final String coordScriptLocation = generateCoordinationScript();

        workers = new IWorker[3];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Worker();
        }

        final ICoordinatorLocal eval = new Coordinator("evalDatabase", "eigg");

        eval.startLocatorServer(34000);

        eval.executeCoordinatorScript(coordScriptLocation);

        eval.blockUntilWorkloadsComplete();
    }

    public String generateCoordinationScript() throws IOException {

        final long runtime = 60000;
        final double probabilityOfFailure = 0.5;
        final long frequencyOfFailure = 30000;
        final int numberOfMachines = 3;
        final int numberOfTables = 1;
        final TableClustering clusteringSpec = new TableClustering(Clustering.GROUPED, 5);

        final Set<WorkloadType> workloadSpecs = new HashSet<WorkloadType>();
        final WorkloadType spec = new WorkloadType(0.5, false, 0, true, 120, LinkToTableLocation.WORKLOAD_PER_TABLE, false);
        workloadSpecs.add(spec);

        return CoordinationScriptGenerator.generateCoordinationScript(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, clusteringSpec, workloadSpecs);
    }
}
