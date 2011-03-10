package org.h2o.db.manager.monitoring.tablemanager;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

/**
 * Stores information on lock requests that have come from a particular user session.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class PerUserQueryMonitoringData {

    /**
     * The user session this object is storing requests from.
     */
    private final LockRequest requestingUser;

    /**
     * History of requests from a particular user session.
     */
    private final Map<Date, LockType> lockRequestHistory = new HashMap<Date, LockType>();

    public PerUserQueryMonitoringData(final LockRequest requestingUser) {

        this.requestingUser = requestingUser;
    }

    /**
     * Add a new request that comes from this user session.
     * @param lockGranted   The type of lock that was requested (and granted).
     */
    public void addLockRequest(final LockType lockGranted) {

        lockRequestHistory.put(new Date(), lockGranted);
    }

    /*
     * Based solely on the requestingUser field.
     */
    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (requestingUser == null ? 0 : requestingUser.hashCode());
        return result;
    }

    /*
     * Based solely on the requestingUser field.
     */
    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final PerUserQueryMonitoringData other = (PerUserQueryMonitoringData) obj;
        if (requestingUser == null) {
            if (other.requestingUser != null) { return false; }
        }
        else if (!requestingUser.equals(other.requestingUser)) { return false; }
        return true;
    }

    public int numberOfRequests() {

        return lockRequestHistory.size();
    }

    public LockRequest getLocation() {

        return requestingUser;
    }

}
