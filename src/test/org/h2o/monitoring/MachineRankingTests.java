package org.h2o.monitoring;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Queue;

import org.h2o.autonomic.numonic.SystemTableDataCollector;
import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.metric.PropertiesFileMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class MachineRankingTests {

    @Before
    public void setUp() throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);
    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * Tests that the ranking works where it is clear that each machine is more powerful than the next.
     * 
     * <p>Requirements: none.
     * <p>Metric: createReplica.metric.
     */
    @Test
    public void basicObviousRanking() throws Exception {

        //Ranking setup.
        final IMetric metric = new PropertiesFileMetric("metric" + File.separator + "createReplica.metric");
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.3, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 9, 9, 0.3, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.3, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(3, rankedInstances.size());

        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
    }

    /**
     * Tests that filtering is performed correctly when there is not enough hard disk space available.
     * 
     * <p>Requirements: 10Gb disk space.
     * <p>Metric: createtReplica.metric.
     */
    @Test
    public void filterTest() throws Exception {

        //Ranking setup.
        final IMetric metric = new PropertiesFileMetric("metric" + File.separator + "createReplica.metric");
        final Requirements requirements = new Requirements(0, 0, 10 * 1024 * 1024, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 1, 0.3, 0.2, 0.99);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 9, 9, 0.3, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.3, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(2, rankedInstances.size());

        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
    }

    /**
     * Tests that machines rank as expected for a CPU intensive workload.
     * <p>Requirements: none.
     * <p>Metric: cpuIntensive.metric
     */
    @Test
    public void cpuIntensiveWorkload() throws Exception {

        //Ranking setup.
        final IMetric metric = new PropertiesFileMetric("metric" + File.separator + "cpuIntensive.metric");
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 11, 10, 1, 0.2, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 20, 9, 0.2, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.3, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(3, rankedInstances.size());

        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
    }

    /**
     * Machine has high capacity, but also high utilization.
     * <p>Requirements: none.
     * <p>Metric: cpuIntensive.metric
     */
    @Test
    public void highCapacityHighUtilization() throws Exception {

        //Ranking setup.
        final IMetric metric = new PropertiesFileMetric("metric" + File.separator + "cpuIntensive.metric");
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.5, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 8, 8, 0.2, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.2, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(3, rankedInstances.size());

        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());
    }

    /**
     * Tests that caching works correctly.
     * <p>Requirements: none.
     * <p>Metric: cpuIntensive.metric
     */
    @Test
    public void caching() throws Exception {

        //Ranking setup.
        final IMetric metric = new PropertiesFileMetric("metric" + File.separator + "cpuIntensive.metric");
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.5, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 8, 8, 0.2, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.2, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Test 1.
        Queue<DatabaseInstanceWrapper> rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(3, rankedInstances.size());

        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());

        //Test 2. (repeat).
        rankedInstances = c.getRankedListOfInstances(metric, requirements);

        assertEquals(3, rankedInstances.size());

        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());

    }
}
