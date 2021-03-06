package org.h2o.autonomic.numonic;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.h2o.autonomic.numonic.interfaces.ICentralDataCollector;
import org.h2o.autonomic.numonic.metric.IMetric;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.autonomic.numonic.ranking.MachineSorter;
import org.h2o.autonomic.numonic.ranking.Requirements;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class SystemTableDataCollector implements ICentralDataCollector {

    Map<DatabaseID, MachineMonitoringData> monitoringData = new HashMap<DatabaseID, MachineMonitoringData>();

    /**
     * Cache of previously computed rankings.
     */
    private final Map<CacheKey, CacheValue> cache = new HashMap<CacheKey, CacheValue>();

    /**
     * Maximum number of entries that can be stored in the cache. It is unlikely that it will get this high,
     * because old entries are constantly being invalidated, so there would have to be more than 10 different request
     * types (metric/requirement combinations) for this to matter.
     */
    private static final int MAXIMUM_CACHE_SIZE = 10;

    /**
     * Used to invalidate cache entries.
     */
    private long timeOfLastUpdate;

    @Override
    public void addMonitoringSummary(final MachineMonitoringData summary) throws RPCException, MovedException {

        monitoringData.remove(summary.getDatabaseID()); //remove a previous summary from this machine if it existed (hash code is based on database ID).
        monitoringData.put(summary.getDatabaseID(), summary); //add the new summary.

        timeOfLastUpdate = System.currentTimeMillis();
    }

    @Override
    public Queue<DatabaseInstanceWrapper> getRankedListOfInstances(final IMetric metric, final Requirements requirements) throws RPCException, MovedException {

        if (metric == null || requirements == null) { throw new RPCException("Null values passed in call to getRankedListOfInstances: [metric=" + metric + ", requirements=" + requirements + "]"); }

        final Queue<DatabaseInstanceWrapper> cachedVersion = getCachedVersion(metric, requirements, cache, timeOfLastUpdate);

        if (cachedVersion != null) {
            return cachedVersion;
        }
        else {

            final Queue<DatabaseInstanceWrapper> rankedMachines = MachineSorter.filterThenRankMachines(requirements, metric, monitoringData.values());

            addToCache(rankedMachines, metric, requirements, cache);

            return rankedMachines;
        }
    }

    @Override
    public void removeDataForInactiveInstance(final DatabaseID inactiveDatabaseID) throws RPCException, MovedException {

        monitoringData.remove(inactiveDatabaseID);

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
    public static Queue<DatabaseInstanceWrapper> getCachedVersion(final IMetric metric, final Requirements requirements, final Map<CacheKey, CacheValue> cache, final long timeOfLastUpdate) {

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
    public static void addToCache(final Queue<DatabaseInstanceWrapper> rankedMachines, final IMetric metric, final Requirements requirements, final Map<CacheKey, CacheValue> cache) {

        if (cache.size() > MAXIMUM_CACHE_SIZE) {
            cache.clear();
        }

        final long timeOfRanking = System.currentTimeMillis();

        cache.put(new CacheKey(metric, requirements), new CacheValue(timeOfRanking, new LinkedList<DatabaseInstanceWrapper>(rankedMachines)));
    }

    /**
     * The key in the Map storing cached machine rankings.
     *
     * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
     */
    public static class CacheKey {

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

    }

    /**
     * The value in the map storing a cache of ranking data.
     *
     * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
     */
    public static class CacheValue {

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

    @Override
    public void excludeInstanceFromRankedResults(final DatabaseID id) throws RPCException, MovedException {

        monitoringData.remove(id);
    }

}
