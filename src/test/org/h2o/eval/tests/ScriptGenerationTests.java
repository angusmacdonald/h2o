package org.h2o.eval.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;

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
