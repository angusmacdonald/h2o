package org.h2o.autonomic.numonic;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.numonic.interfaces.ICentralDataCollector;
import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.autonomic.numonic.ranking.MachineSorter;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class SystemTableDataCollctor implements ICentralDataCollector {

    Set<MachineMonitoringData> monitoringData = new HashSet<MachineMonitoringData>();

    private final Requirements requirements;

    private final IMetric defaultSortMetric;

    /**
     * @param requirements  Default minimum requirements for database instances. Instances failing to meet
     * these requirements (in terms of CPU, memory, and disk capacity) will be filtered out before
     * the remaining instances are sorted.
     * @param defaultSortMetric The default metric used to rank the instances being sorted. Used if no metric was specified in
     * the call to {@link #getRankedListOfInstances(IMetric)}, or used always in the call to {@link #getRankedListOfInstances()}.
     */
    public SystemTableDataCollctor(final Requirements requirements, final IMetric defaultSortMetric) {

        this.requirements = requirements;
        this.defaultSortMetric = defaultSortMetric;
    }

    @Override
    public void addMonitoringSummary(final MachineMonitoringData summary) throws RPCException, MovedException {

        monitoringData.remove(summary); //remove a previous summary from this machine if it existed (hash code is based on database ID).
        monitoringData.add(summary); //add the new summary.

    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances() {

        return MachineSorter.filterThenRankMachines(requirements, defaultSortMetric, monitoringData);

    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances(IMetric metric) throws RPCException, MovedException {

        if (metric == null) { //if the requesting instance wasn't able to create the metric (e.g. properties file not found), use the default one.
            metric = defaultSortMetric;
        }

        return MachineSorter.filterThenRankMachines(requirements, metric, monitoringData);
    }

}
