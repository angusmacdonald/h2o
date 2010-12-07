package org.h2o.db.manager.monitoring;

import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

/**
 * Represents the percentage of lock requests for a particular table that come from a particular machine.
 * 
 * Relationship: one {@link TableManagerRemote} -> many {@link LockRequestPercentagesPerInstance} objects -> one {@link DatabaseInstanceWrapper} each.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LockRequestPercentagesPerInstance implements Comparable<LockRequestPercentagesPerInstance> {

    private final DatabaseInstanceWrapper instance;
    private final double percentageOfRequests;

    /**
     * @param instance
     * @param percentageOfRequests
     */
    public LockRequestPercentagesPerInstance(final DatabaseInstanceWrapper instance, final double percentageOfRequests) {

        this.instance = instance;
        this.percentageOfRequests = percentageOfRequests;
    }

    public DatabaseInstanceWrapper getInstance() {

        return instance;
    }

    public double getPercentageOfRequests() {

        return percentageOfRequests;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (instance == null ? 0 : instance.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final LockRequestPercentagesPerInstance other = (LockRequestPercentagesPerInstance) obj;
        if (instance == null) {
            if (other.instance != null) { return false; }
        }
        else if (!instance.equals(other.instance)) { return false; }
        return true;
    }

    @Override
    public int compareTo(final LockRequestPercentagesPerInstance o) {

        if (o == null) { return 1; }

        if (getPercentageOfRequests() > o.getPercentageOfRequests()) { return 1; }

        if (getPercentageOfRequests() < o.getPercentageOfRequests()) { return 0; }

        return 0;
    }
}
