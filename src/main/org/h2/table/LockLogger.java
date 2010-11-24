package org.h2.table;

import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Session;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

public class LockLogger {

    private static Map<String, LockLogger> logs = new HashMap<String, LockLogger>();
    private final StringBuffer lockHistory = new StringBuffer();

    private final boolean enabled;
    private final String tableName;

    private LockLogger(final boolean enabled, final String tableName) {

        this.enabled = enabled;
        this.tableName = tableName;

        logs.put(tableName, this);
    }

    public static LockLogger getLogger(final boolean enabled, final String tableName) {

        LockLogger logger = logs.get(tableName);
        if (logger == null) {
            logger = new LockLogger(enabled, tableName);
        }

        return logger;
    }

    public static void dumpLockHistory(final String tableName) {

        final LockLogger logger = logs.get(tableName);
        if (logger == null) {
            System.out.println("no H2 lock history for table: " + tableName);
        }
        else {
            logger.dumpLockHistory();
        }
    }

    public void dumpLockHistory() {

        if (!enabled) {
            System.out.print("no ");
        }
        System.out.println("lock history for table: " + tableName);
        System.out.println(lockHistory);
    }

    public void prelock(final Session session, final boolean exclusive) {

        if (enabled) {
            lockHistory.append("\nTIME: " + System.currentTimeMillis() + "\n");
            lockHistory.append("H2 REQUEST: " + (exclusive ? "exclusive" : "shared") + "\n");
            lockHistory.append("session: " + session + "\n");
        }
    }

    public void prelock(final LockType lockType, final LockRequest lockRequest, final LockType requestResult) {

        if (enabled) {
            lockHistory.append("\nTIME: " + System.currentTimeMillis() + "\n");
            lockHistory.append("H2O REQUEST: lock type: " + lockType + "\n");
            lockHistory.append("lock request: " + lockRequest + "\n");
            lockHistory.append("result: " + requestResult + "\n");
        }
    }

    public void postlock(final Session session, final boolean exclusive, final Session result) {

        if (enabled) {
            lockHistory.append("\nTIME: " + System.currentTimeMillis() + "\n");
            lockHistory.append("H2 REPLY: " + (exclusive ? "exclusive" : "shared") + "\n");
            lockHistory.append("session: " + session + "\n");
            lockHistory.append("result: " + result + "\n");
        }
    }

    public void unlock(final Session session, final boolean exclusive) {

        if (enabled) {
            lockHistory.append("\nTIME: " + System.currentTimeMillis() + "\n");
            lockHistory.append("H2 RELEASE:\n");

            if (exclusive) {
                lockHistory.append("released exclusive lock\n");
            }
            else {
                lockHistory.append("released shared lock\n");
            }

            lockHistory.append("session: " + session + "\n");
        }
    }

    public void unlock(final LockRequest lockRequest, final LockType requestResult) {

        if (enabled) {
            lockHistory.append("\nTIME: " + System.currentTimeMillis() + "\n");
            lockHistory.append("H2O RELEASE:\n");
            lockHistory.append("lock request: " + lockRequest + "\n");
            lockHistory.append("result: " + requestResult + "\n");
        }
    }
}
