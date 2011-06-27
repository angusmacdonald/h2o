package org.h2o.eval.tests;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.PrettyPrinter;

/**
 * Tests of script generation. NOTE: these tests do very little checking of the contents of generated scripts, 
 * so a visual inspection of the output is currently needed to confirm their correctness. //TODO fix this.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class ScriptGenerationTests {

    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

    @Test
    public void testSingleWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.ALL_ENCOMPASSING_WORKLOAD, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping, createFolderPath());

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(1, createWorkloads.size());
    }

    public String createFolderPath() {

        return "generatedWorkloads" + File.separator + dateFormatter.format(System.currentTimeMillis());
    }

    @Test
    public void testGroupedWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.GROUPED_WORKLOAD, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping, createFolderPath());

        System.out.println(PrettyPrinter.toString(createWorkloads));

        assertEquals(2, createWorkloads.size());
    }

    @Test
    public void testPerTableWorkloadFile() throws IOException {

        final WorkloadGenerator gen = new WorkloadGenerator();

        final WorkloadType spec = new WorkloadType(0.5, false, 50, true, 5, LinkToTableLocation.WORKLOAD_PER_TABLE, true);
        final TableGrouping tableGrouping = createTestTableGrouping();

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping, createFolderPath());

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

        final Map<String, Integer> createWorkloads = gen.createWorkloads(spec, tableGrouping, createFolderPath());

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
