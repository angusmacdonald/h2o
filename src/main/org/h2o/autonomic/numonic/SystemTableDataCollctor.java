package org.h2o.autonomic.numonic;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.h2o.autonomic.numonic.interfaces.ICentralDataCollector;
import org.h2o.autonomic.numonic.ranking.IMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.autonomic.numonic.ranking.MachineSorter;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class SystemTableDataCollctor implements ICentralDataCollector {

    Set<MachineMonitoringData> monitoringData = new HashSet<MachineMonitoringData>();

    private final Requirements requirements;

    private final IMetric sort_metric;

    /**
     * @param requirements  Default minimum requirements for database instances. Instances failing to meet
     * these requirements (in terms of CPU, memory, and disk capacity) will be filtered out before
     * the remaining instances are sorted.
     * @param sort_metric The method used to rank the instances being sorted.
     */
    public SystemTableDataCollctor(final Requirements requirements, final IMetric sort_metric) {

        this.requirements = requirements;
        this.sort_metric = sort_metric;
    }

    @Override
    public void addMonitoringSummary(final MachineMonitoringData summary) throws RPCException, MovedException {

        monitoringData.remove(summary); //remove a previous summary from this machine if it existed.
        monitoringData.add(summary); //add the new summary.

    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances() {

        return MachineSorter.filterThenRankMachines(requirements, sort_metric, monitoringData);

    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances(final IMetric metric) throws RPCException, MovedException {

        return MachineSorter.filterThenRankMachines(requirements, metric, monitoringData);
    }

}
