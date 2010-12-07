package org.h2o.db.manager.monitoring;

import java.util.SortedSet;

import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

public interface ITableManagerMonitor {

    /**
     * Add information on a new query/update received by the Table Manager.
     * @param requestingUser    The name of the session-database that is making the request.
     * @param typeOfQuery       The type of query being made.
     */
    public void addQueryInformation(LockRequest requestingUser, LockType typeOfQuery);

    /**
     * Get the ratio of reads to writes.
     * @return number of reads monitored divided by number of writes monitored.
     */
    public double getReadWriteRatio();

    /**
     * Get the database instance / session that has accessed this table most often.
     * @return
     */
    public LockRequest getMostCommonQueryLocation();

    /**
     * Get the percentage of all lock requests that come each database instance.
     * @return  The percentage of all lock requests monitored that come from each database instance.
     */
    public SortedSet<LockRequestPercentagesPerInstance> getPercentageOfLockRequestsFromInstances();

}
