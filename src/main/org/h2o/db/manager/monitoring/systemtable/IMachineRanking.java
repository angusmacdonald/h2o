package org.h2o.db.manager.monitoring.systemtable;

import java.util.SortedSet;

/**
 * IMPLEMENTED BY THE SYSTEM TABLE.
 * 
 * <p>Interface for all of the System Table's operations involving resource monitoring information.
 * 
 * <p>These methods allow H2O instances to send monitoring information to the System Table, and allow them
 * to request summaries of this information.</p>
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface IMachineRanking {

    /**
     * Add summary information (detailing the availability of a machine at a particular instance) to the System Table's
     * monitoring.
     * @param summary A resource summary from a single database instance.
     */
    public void addMonitoringSummary(InstanceResourceSummary summary);

    /**
     * Get the set of database instances in the database system, sorted by their availability: most available first.
     * 
     * <p>These are sorted based on an availability metric, described here: XXX. //TODO add link.
     * @return Ranked set of H2O instances.
     */
    public SortedSet<InstanceResourceSummary> getRankedListOfInstances();
}
