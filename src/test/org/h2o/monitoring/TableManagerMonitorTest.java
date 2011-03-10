package org.h2o.monitoring;

import static org.junit.Assert.assertEquals;

import java.util.SortedSet;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.monitoring.LockRequestPercentagesPerInstance;
import org.h2o.db.manager.monitoring.TableManagerMonitor;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TableManagerMonitorTest {

    private TableManagerMonitor monitor = null;

    final DatabaseInstanceWrapper dbWrapperOne = new DatabaseInstanceWrapper(DatabaseID.parseURL("jdbc:h2:mem:one"), null, true);
    final DatabaseInstanceWrapper dbWrapperTwo = new DatabaseInstanceWrapper(DatabaseID.parseURL("jdbc:h2:mem:two"), null, true);
    final DatabaseInstanceWrapper dbWrapperThree = new DatabaseInstanceWrapper(DatabaseID.parseURL("jdbc:h2:mem:three"), null, true);

    final LockRequest lockRequestFromOne = new LockRequest(dbWrapperOne, 1);
    final LockRequest lockRequestFromOneSecondSession = new LockRequest(dbWrapperOne, 2);

    final LockRequest lockRequestFromTwo = new LockRequest(dbWrapperTwo, 1);

    final LockRequest lockRequestFromThree = new LockRequest(dbWrapperThree, 1);
    final LockRequest lockRequestFromThreeSecondSession = new LockRequest(dbWrapperThree, 2);

    @Before
    public void setUp() throws Exception {

        monitor = new TableManagerMonitor();
    }

    @After
    public void tearDown() throws Exception {

    }

    /*
     * Tests of the getReadWriteRatio() method. 
     */

    @Test
    public void testAllWrites() {

        insertQueries(100, LockType.WRITE, lockRequestFromOne);

        assertEquals(0, monitor.getReadWriteRatio(), 0);

    }

    @Test
    public void testAllReads() {

        insertQueries(100, LockType.READ, lockRequestFromOne);

        assertEquals(Double.POSITIVE_INFINITY, monitor.getReadWriteRatio(), 0);

    }

    @Test
    public void testFiftyFiftyReadWrite() {

        final int numberOfReads = 100;

        insertQueries(numberOfReads, LockType.READ, lockRequestFromOne);

        insertQueries(100, LockType.WRITE, lockRequestFromOne);

        assertEquals(1, monitor.getReadWriteRatio(), 0);

    }

    @Test
    public void testManyReadsOneWrite() {

        insertQueries(100, LockType.READ, lockRequestFromOne);

        insertQueries(1, LockType.WRITE, lockRequestFromOne);

        assertEquals(100, monitor.getReadWriteRatio(), 0);

    }

    /*
     * Tests of the getMostCommonQueryLocation() method. 
     */

    @Test
    public void testGetMostCommonQueryLocationOneLocation() {

        insertQueries(1, LockType.READ, lockRequestFromOne);

        assertEquals(lockRequestFromOne, monitor.getMostCommonQueryLocation());

    }

    @Test
    public void testGetMostCommonQueryLocationManyLocations() {

        insertQueries(1, LockType.READ, lockRequestFromOne);
        insertQueries(1, LockType.READ, lockRequestFromOneSecondSession);
        insertQueries(3, LockType.READ, lockRequestFromTwo);
        insertQueries(1, LockType.READ, lockRequestFromThree);

        assertEquals(lockRequestFromTwo, monitor.getMostCommonQueryLocation());

    }

    /*
     * Tests of the getMostCommonQueryLocation() method. 
     */

    @Test
    public void testPercentages() {

        insertQueries(4, LockType.READ, lockRequestFromOne);
        insertQueries(1, LockType.READ, lockRequestFromOneSecondSession);
        insertQueries(2, LockType.READ, lockRequestFromTwo);
        insertQueries(1, LockType.READ, lockRequestFromThree);

        final SortedSet<LockRequestPercentagesPerInstance> percentages = monitor.getPercentageOfLockRequestsFromInstances();

        LockRequestPercentagesPerInstance firstByRequest = percentages.last();

        assertEquals(dbWrapperOne, firstByRequest.getInstance());
        assertEquals(.625, firstByRequest.getPercentageOfRequests(), 0);

        percentages.remove(firstByRequest);
        firstByRequest = percentages.last();

        assertEquals(dbWrapperTwo, firstByRequest.getInstance());
        assertEquals(.25, firstByRequest.getPercentageOfRequests(), 0);
    }

    /*
     * Utility Functions...
     */

    private void insertQueries(final int numberOfInsertions, final LockType typeOfInsertion, final LockRequest locationOfInsertion) {

        for (int i = 0; i < numberOfInsertions; i++) {
            monitor.addQueryInformation(locationOfInsertion, typeOfInsertion);
            sleep(1);
        }
    }

    private void sleep(final int i) {

        try {
            Thread.sleep(i);
        }
        catch (final InterruptedException e) {
            e.printStackTrace();
        }
    }
}
