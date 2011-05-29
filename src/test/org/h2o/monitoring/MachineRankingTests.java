package org.h2o.monitoring;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.numonic.SystemTableDataCollector;
import org.h2o.autonomic.numonic.metric.CpuIntensiveMetric;
import org.h2o.autonomic.numonic.metric.CreateReplicaMetric;
import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.metric.MemIntensiveMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.manager.SystemTable;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class MachineRankingTests {

    @Before
    public void setUp() throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);
    }

    /**
     * Tests that an exception is thrown if the requirements and sort metric provided are null.
     * <p>Requirements: <null>
     * <p>Metric: <null>
     */
    @Test(expected = RPCException.class)
    public void nullCheck() throws Exception {

        //Ranking setup.
        final IMetric metric = null;
        final Requirements requirements = null;
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.5, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 8, 8, 0.2, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.2, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        //Test 1.
        c.getRankedListOfInstances(metric, requirements);

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
        final IMetric metric = new CreateReplicaMetric();
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
        final IMetric metric = new CreateReplicaMetric();
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
        final IMetric metric = new CpuIntensiveMetric();
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
        final IMetric metric = new CpuIntensiveMetric();
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
     * Tests that caching works correctly when the same call is made twice.
     * <p>Requirements: none.
     * <p>Metric: cpuIntensive.metric
     */
    @Test
    public void cachingDoubleCall() throws Exception {

        //Ranking setup.
        final IMetric metric = new CpuIntensiveMetric();
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

    /**
     * Tests that caching works correctly when different calls are made (i.e. the cache shouldn't be used).
     * <p>Requirements: none.
     * <p>Metric: cpuIntensive.metric
     */
    @Test
    public void cachingDifferentCalls() throws Exception {

        //Ranking setup.
        final IMetric metric = new CpuIntensiveMetric();
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        final IMetric metric2 = new MemIntensiveMetric();
        final Requirements requirements2 = new Requirements(0, 0, 0, 0);

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.5, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 8, 8, 0.2, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 12, 8, 0.2, 0.2, 0.2);

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
        rankedInstances = c.getRankedListOfInstances(metric2, requirements2);

        assertEquals(3, rankedInstances.size());

        assertEquals(m3.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());

    }

    /**
     * Tests that removing non-active database instances doesn't upset the ordering of the queue.
     * 
     * <p>Requirements: none.
     * <p>Metric: createReplica.metric.
     */
    @Test
    public void removeInactiveInstances() throws Exception {

        //Ranking setup.
        final IMetric metric = new CreateReplicaMetric();
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.3, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 9, 9, 0.3, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.3, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        final Set<DatabaseInstanceWrapper> activeInstances = new HashSet<DatabaseInstanceWrapper>();
        activeInstances.add(m1.getDatabaseWrapper());
        activeInstances.add(m2.getDatabaseWrapper());

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = SystemTable.removeInactiveInstances(c.getRankedListOfInstances(metric, requirements), c, activeInstances);

        assertEquals(2, rankedInstances.size());

        assertEquals(m1.getDatabaseID(), rankedInstances.remove().getURL());
        assertEquals(m2.getDatabaseID(), rankedInstances.remove().getURL());
    }

    /**
     * Tests that instances for which there is no monitoring information are added to the end of the queue.
     * 
     * <p>Requirements: none.
     * <p>Metric: createReplica.metric.
     */
    @Test
    public void addUnomonitoredInstances() throws Exception {

        //Ranking setup.
        final IMetric metric = new CreateReplicaMetric();
        final Requirements requirements = new Requirements(0, 0, 0, 0);
        final SystemTableDataCollector c = new SystemTableDataCollector();

        //Machines
        final MachineMonitoringData m1 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:one", 10, 10, 10, 0.3, 0.2, 0.2);
        final MachineMonitoringData m2 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:two", 9, 9, 9, 0.3, 0.2, 0.2);
        final MachineMonitoringData m3 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:three", 8, 8, 8, 0.3, 0.2, 0.2);

        final MachineMonitoringData m4 = ResourceSpec.generateMonitoringData("jdbc:h2o:mem:four", 8, 8, 8, 0.3, 0.2, 0.2);

        c.addMonitoringSummary(m1);
        c.addMonitoringSummary(m2);
        c.addMonitoringSummary(m3);

        final Set<DatabaseInstanceWrapper> activeInstances = new HashSet<DatabaseInstanceWrapper>();
        activeInstances.add(m1.getDatabaseWrapper());
        activeInstances.add(m2.getDatabaseWrapper());

        //Tests
        final Queue<DatabaseInstanceWrapper> rankedInstances = SystemTable.removeInactiveInstances(c.getRankedListOfInstances(metric, requirements), c, activeInstances);

        assertEquals(2, rankedInstances.size());

        final Set<DatabaseInstanceWrapper> unMonitored = new HashSet<DatabaseInstanceWrapper>();
        unMonitored.add(m4.getDatabaseWrapper());

        final Queue<DatabaseInstanceWrapper> unMonitoringInstancesIncluded = SystemTable.addUnMonitoredMachinesToEndOfQueue(rankedInstances, unMonitored);

        assertEquals(3, unMonitoringInstancesIncluded.size());

        assertEquals(m1.getDatabaseID(), unMonitoringInstancesIncluded.remove().getURL());
        assertEquals(m2.getDatabaseID(), unMonitoringInstancesIncluded.remove().getURL());
        assertEquals(m4.getDatabaseID(), unMonitoringInstancesIncluded.remove().getURL());
    }

}
