package org.h2o.autonomic.numonic.interfaces;

import java.util.SortedSet;

import org.h2o.autonomic.numonic.ranking.IMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

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
public interface ICentralDataCollector {

    /**
     * Add summary information (detailing the availability of a machine at a particular instance) to the System Table's
     * monitoring.
     * @param summary A resource summary from a single database instance.
     * @throws MovedException 
     * @throws RPCException 
     */
    public void addMonitoringSummary(MachineMonitoringData summary) throws RPCException, MovedException;

    /**
     * Get the set of database instances in the database system, sorted by their availability: most available first.
     * 
     * <p>These are sorted based on an availability metric, described in {@link IMetric}
     * @return Ranked set of H2O instances.
     * @throws MovedException 
     * @throws RPCException 
     */
    public SortedSet<MachineMonitoringData> getRankedListOfInstances() throws RPCException, MovedException;
}
