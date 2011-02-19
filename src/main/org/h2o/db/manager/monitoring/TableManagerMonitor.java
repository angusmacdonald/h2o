package org.h2o.db.manager.monitoring;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public class TableManagerMonitor implements ITableManagerMonitor {

    /**
     * Stores information on the location of requests coming into the Table Manager.
     */
    private final Map<LockRequest, PerUserQueryMonitoringData> perUserData = new HashMap<LockRequest, PerUserQueryMonitoringData>();

    /*
     * Read-Write Ratio.
     */

    /**
     * The number of incoming read requests since monitoring began.
     */
    private int numberOfReads = 0;

    /**
     * The number of incoming write requests since monitoring began.
     */
    private int numberOfWrites = 0;

    @Override
    public void addQueryInformation(final LockRequest requestingUser, final LockType typeOfQuery) {

        PerUserQueryMonitoringData monitoringData = perUserData.get(requestingUser);

        if (monitoringData == null) {
            monitoringData = new PerUserQueryMonitoringData(requestingUser);
            perUserData.put(requestingUser, monitoringData);
        }

        monitoringData.addLockRequest(typeOfQuery);

        if (typeOfQuery == LockType.WRITE) {
            numberOfWrites++;
        }
        else if (typeOfQuery == LockType.READ) {
            numberOfReads++;
        }
    }

    @Override
    public double getReadWriteRatio() {

        if (numberOfWrites == 0 && numberOfReads > 0) { return Double.POSITIVE_INFINITY; }

        if (numberOfWrites == 0 && numberOfReads == 0) { return 1; }

        return numberOfReads / (double) numberOfWrites;
    }

    @Override
    public LockRequest getMostCommonQueryLocation() {

        PerUserQueryMonitoringData highestCountLocation = null;
        int highestCountValue = 0;

        for (final PerUserQueryMonitoringData dataItem : perUserData.values()) {
            if (dataItem.numberOfRequests() > highestCountValue) {
                highestCountValue = dataItem.numberOfRequests();
                highestCountLocation = dataItem;
            }
        }

        return highestCountLocation.getLocation();
    }

    @Override
    public SortedSet<LockRequestPercentagesPerInstance> getPercentageOfLockRequestsFromInstances() {

        final int totalNumberOfRequests = numberOfReads + numberOfWrites;

        final Map<DatabaseInstanceWrapper, Integer> queryCount = new HashMap<DatabaseInstanceWrapper, Integer>();

        for (final PerUserQueryMonitoringData dataItem : perUserData.values()) {

            if (queryCount.get(dataItem.getLocation().getRequestLocation()) == null) {
                queryCount.put(dataItem.getLocation().getRequestLocation(), dataItem.numberOfRequests());
            }
            else {
                final Integer numberOfRequests = queryCount.get(dataItem.getLocation().getRequestLocation());
                queryCount.put(dataItem.getLocation().getRequestLocation(), numberOfRequests + dataItem.numberOfRequests());
            }
        }

        final SortedSet<LockRequestPercentagesPerInstance> percentages = new TreeSet<LockRequestPercentagesPerInstance>();

        for (final Entry<DatabaseInstanceWrapper, Integer> instance : queryCount.entrySet()) {
            final LockRequestPercentagesPerInstance lockRequestPercentages = new LockRequestPercentagesPerInstance(instance.getKey(), instance.getValue() / (double) totalNumberOfRequests);
            final boolean added = percentages.add(lockRequestPercentages);

            assert added : "The object being added should always be unique here.";
        }

        return percentages;
    }
}
