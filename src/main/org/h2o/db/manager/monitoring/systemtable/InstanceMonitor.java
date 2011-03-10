package org.h2o.db.manager.monitoring.systemtable;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

public class InstanceMonitor implements IMachineRanking {

    /**
     * Summaries of resource information from each database instance.
     */
    List<InstanceResourceSummary> summaries;

    public InstanceMonitor() {

        summaries = new LinkedList<InstanceResourceSummary>();
    }

    @Override
    public void addMonitoringSummary(final InstanceResourceSummary summary) {

        summaries.add(summary);
    }

    @Override
    public SortedSet<InstanceResourceSummary> getRankedListOfInstances() {

        return null; //TODO implement.
    }

}
