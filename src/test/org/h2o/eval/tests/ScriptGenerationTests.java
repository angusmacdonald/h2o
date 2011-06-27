package org.h2o.eval.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2o.eval.script.coord.CoordinationScriptGenerator;
import org.h2o.eval.script.coord.specification.TableClustering;
import org.h2o.eval.script.coord.specification.TableClustering.Clustering;
import org.h2o.eval.script.coord.specification.TableGrouping;
import org.h2o.eval.script.coord.specification.WorkloadType;
import org.h2o.eval.script.coord.specification.WorkloadType.LinkToTableLocation;
import org.h2o.eval.script.workload.WorkloadGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.PrettyPrinter;

public class ScriptGenerationTests {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSingleWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.ALL_ENCOMPASSING_WORKLOAD, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping);

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(1, createWorkloads.size());
    }

    @Test
    public void testGroupedWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.GROUPED_WORKLOAD, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping);

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(2, createWorkloads.size());
    }

    @Test
    public void testPerTableWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.WORKLOAD_PER_TABLE, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping);

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(5, createWorkloads.size());
    }

    /**
     * Tests that workloads aren't to be located on the same machine as the table they are querying.
     * @throws IOException
     */
    @Test
    public void testNonLocalWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.WORKLOAD_PER_TABLE, false);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping);

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(5, createWorkloads.size());
    }

    /**
     * Creates a test co-ordination script.
     * @throws IOException
     */
    @Test
    public void testCoordinationScriptGeneration() throws IOException {

        final long runtime = 60000;
        final double probabilityOfFailure = 0.1;
        final double frequencyOfFailure = 10000;
        final int numberOfMachines = 5;
        final int numberOfTables = 3;
        final TableClustering clusteringSpec = new TableClustering(Clustering.GROUPED, 2);

        final Set<WorkloadType> workloadSpecs = new HashSet<WorkloadType>();
        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.GROUPED_WORKLOAD, true);
        workloadSpecs.add(spec);

        final String coordinationScriptLocation = CoordinationScriptGenerator.generateCoordinationScript(runtime, probabilityOfFailure, frequencyOfFailure, numberOfMachines, numberOfTables, clusteringSpec, workloadSpecs);

        System.out.println(coordinationScriptLocation);

    }

    /*
     * Utility methods.
     */
    private TableGrouping createTestTableGrouping() {

        final TableGrouping tableGrouping = new TableGrouping();

        tableGrouping.addTable(0, "test0");
        tableGrouping.addTable(0, "test1");
        tableGrouping.addTable(0, "test2");
        tableGrouping.addTable(1, "test3");
        tableGrouping.addTable(1, "test4");
        return tableGrouping;
    }
}
