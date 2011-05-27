package org.h2o.autonomic.numonic.ranking;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class MachineSorter {

    /**
    * Given resource monitoring data from a database this method returns a sorted set of machines (for which monitoring data is available),
    * with the top machines being the most appropriate to execute the workload defined by the sort_metric parameter. Machines that don't
    * meet a set of basic requirements (final the requirements parameter) are filtered out.
    *
    * @param requirements a set of minimum requirements for the machines being compared
    * @param sort_metric the metric by which machines will be sorted
    * @param machineData  The machines which must be filtered and ranked.
    * @return a sorted set of machines
    */
    public static Queue<DatabaseInstanceWrapper> filterThenRankMachines(final Requirements requirements, final IMetric sort_metric, final Collection<MachineMonitoringData> machineData) {

        // Filter Machines.
        final Set<MachineMonitoringData> filteredInstances = filterMachines(requirements, machineData);
        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Instances remaining after filtering: " + filteredInstances.size() + "/" + machineData.size());

        // Rank Machines.
        final MachineRanker ranker = new MachineRanker(sort_metric);

        return ranker.sortMachines(filteredInstances);

    }

    /**
     * Remove instances from the set of available machines if they do not meet the minimum requirements for the given task.
     * @param requirements  Minimum required compute/memory/storage capacity.
     * @param machineData   Collection of all available machines.
     * @return Filtered collection of all available machines.
     */
    private static Set<MachineMonitoringData> filterMachines(final Requirements requirements, final Collection<MachineMonitoringData> machineData) {

        final MachineFilter filter = new MachineFilter();

        final Set<MachineMonitoringData> filteredInstances = filter.selectSuitableMachines(machineData, requirements);

        return filteredInstances;
    }
}
