package org.h2o.autonomic.numonic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

public class SystemTableDataCollector implements ICentralDataCollector {

    Set<MachineMonitoringData> monitoringData = new HashSet<MachineMonitoringData>();

    private final Map<CacheKey, CacheValue> cache = new HashMap<CacheKey, CacheValue>();

    private long timeOfLastUpdate;

    @Override
    public void addMonitoringSummary(final MachineMonitoringData summary) throws RPCException, MovedException {

        monitoringData.remove(summary); //remove a previous summary from this machine if it existed (hash code is based on database ID).
        monitoringData.add(summary); //add the new summary.

        timeOfLastUpdate = System.currentTimeMillis();
    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances(final IMetric metric, final Requirements requirements) throws RPCException, MovedException {

        if (metric == null || requirements == null) { throw new RPCException("Null values passed in call to getRankedListOfInstances: [metric=" + metric + ", requirements=" + requirements + "]"); }

        final Queue<DatabaseInstanceWrapper> cachedVersion = getCachedVersion(metric, requirements);

        if (cachedVersion != null) {
            return cachedVersion;
        }
        else {

            final Queue<DatabaseInstanceWrapper> rankedMachines = MachineSorter.filterThenRankMachines(requirements, metric, monitoringData);

            cacheMachineRanking(rankedMachines, metric, requirements);

            return rankedMachines;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Caching Code.
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Check whether a cached version exists for this metric, and that it is up-to-date (i.e. no other monitoring updates have been 
     * made since this version was cached. 
     * @param metric the metric which produced this result.
     * @param requirements 
     * @return cache of the results of running the specified metric over monitoring data. Null if no cached version exists.
     */
    private Queue<DatabaseInstanceWrapper> getCachedVersion(final IMetric metric, final Requirements requirements) {

        final CacheKey key = new CacheKey(metric, requirements);

        final CacheValue cacheValue = cache.get(key);

        if (cacheValue != null && cacheValue.timeOfRanking > timeOfLastUpdate) {
            return cacheValue.rankedMachines;
        }
        else if (cacheValue != null) {
            cache.remove(key);
        }

        return null;
    }

    /**
     * Add a recently computed ranking to the cache.
     * @param rankedMachines machines that have recently been ranked. 
     * @param metric the metric used to rank these machines.
     * @param requirements 
     */
    private void cacheMachineRanking(final Queue<DatabaseInstanceWrapper> rankedMachines, final IMetric metric, final Requirements requirements) {

        final long timeOfRanking = System.currentTimeMillis();

        cache.put(new CacheKey(metric, requirements), new CacheValue(timeOfRanking, rankedMachines));
    }

    /**
     * The key in the Map storing cached machine rankings.
     *
     * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
     */
    private class CacheKey {

        public final IMetric metric;
        public final Requirements requirements;

        /**
         * @param metric
         * @param requirements
         * @param timeOfRanking
         */
        public CacheKey(final IMetric metric, final Requirements requirements) {

            this.metric = metric;
            this.requirements = requirements;
        }

        @Override
        public int hashCode() {

            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + (metric == null ? 0 : metric.hashCode());
            result = prime * result + (requirements == null ? 0 : requirements.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {

            if (this == obj) { return true; }
            if (obj == null) { return false; }
            if (getClass() != obj.getClass()) { return false; }
            final CacheKey other = (CacheKey) obj;
            if (!getOuterType().equals(other.getOuterType())) { return false; }
            if (metric == null) {
                if (other.metric != null) { return false; }
            }
            else if (!metric.equals(other.metric)) { return false; }
            if (requirements == null) {
                if (other.requirements != null) { return false; }
            }
            else if (!requirements.equals(other.requirements)) { return false; }
            return true;
        }

        private SystemTableDataCollector getOuterType() {

            return SystemTableDataCollector.this;
        }

    }

    /**
     * The value in the map storing a cache of ranking data.
     *
     * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
     */
    private class CacheValue {

        public final long timeOfRanking;
        public final Queue<DatabaseInstanceWrapper> rankedMachines;

        /**
         * @param timeOfRanking
         * @param rankedMachines
         */
        public CacheValue(final long timeOfRanking, final Queue<DatabaseInstanceWrapper> rankedMachines) {

            this.timeOfRanking = timeOfRanking;
            this.rankedMachines = rankedMachines;
        }

    }
}
