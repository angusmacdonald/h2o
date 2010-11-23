package org.h2.table;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.util.ObjectArray;

public class H2LockManager {

    private Session sessionHoldingExclusiveLock;

    private final Set<Session> sessionsHoldingSharedLocks = new HashSet<Session>();
    private final Map<Session, Table> sessionsWaitingForLocks = new HashMap<Session, Table>();

    private final TableData tableData;
    private final Database database;
    private final Trace traceLock;

    // -------------------------------------------------------------------------------------------------------

    public H2LockManager(final TableData tableData, final Database database) {

        this.tableData = tableData;
        this.database = database;
        traceLock = database.getTrace(Trace.LOCK);
    }

    // -------------------------------------------------------------------------------------------------------

    public Session lock(final Session session, final boolean exclusive, final boolean force) throws SQLException {

        final int lockMode = database.getLockMode();

        if (lockMode != Constants.LOCK_MODE_OFF) {
            synchronized (database) {
                try {
                    return obtainLock(session, exclusive);
                }
                finally {
                    sessionsWaitingForLocks.remove(session);
                }
            }
        }

        return null;
    }

    public boolean isLockedExclusivelyBy(final Session session) {

        return sessionHoldingExclusiveLock == session;
    }

    public void recordExclusiveLock(final Session sessionHoldingExclusiveLock) {

        this.sessionHoldingExclusiveLock = sessionHoldingExclusiveLock;
    }

    public Session getSessionHoldingExclusiveLock() {

        return sessionHoldingExclusiveLock;
    }

    public boolean isLockedExclusively() {

        return sessionHoldingExclusiveLock != null;
    }

    public void releaseAllLocks() {

        synchronized (database) {
            releaseExclusiveLock();
            releaseAllSharedLocks();
        }
    }

    public void unlock(final Session s) {

        synchronized (database) {

            traceLock(s, isLockedExclusivelyBy(s), "unlock");

            if (isLockedExclusivelyBy(s)) {
                releaseExclusiveLock();
            }

            if (isLockedSharedBy(s)) {
                releaseSharedLock(s);
            }

            if (database.getSessionCount() > 1) {
                database.notifyAll();
            }
        }
    }

    // -------------------------------------------------------------------------------------------------------

    private boolean recordSharedLock(final Session session) {

        return sessionsHoldingSharedLocks.add(session);
    }

    private boolean isLockedSharedBy(final Session session) {

        return sessionsHoldingSharedLocks.contains(session);
    }

    private boolean noSessionHoldsSharedLock() {

        return sessionsHoldingSharedLocks.isEmpty();
    }

    private boolean noSessionHoldsExclusiveLock() {

        return getSessionHoldingExclusiveLock() == null;
    }

    private boolean releaseSharedLock(final Session s) {

        return sessionsHoldingSharedLocks.remove(s);
    }

    private void releaseExclusiveLock() {

        recordExclusiveLock(null);
    }

    private void releaseAllSharedLocks() {

        sessionsHoldingSharedLocks.clear();
    }

    // -------------------------------------------------------------------------------------------------------

    private Session obtainLock(Session session, final boolean exclusive) throws SQLException {

        final long max = System.currentTimeMillis() + session.getLockTimeout();

        boolean checkDeadlock = false;
        while (true) {

            if (sessionHoldingExclusiveLock != null && sessionHoldingExclusiveLock != session) {

                assert tableData.getName().equals("SYS") : "lock stealing only permitted for table SYS";

                /* 
                  * H2O hack. It ensures that A-B-A communication doesn't lock up the DB (normally through the SYS table), as the returning update can use the same session
                  * as the originating update (which has all of the pertinent locks).
                  * 
                  * This happens when the PUBLIC.SYS table is altered (after almost every update through the Database.remoteMeta() [line 1266] method. This is called from 
                  * TableData.validateConvertUpdateSequence -> Column -> ... -> Sequence.flush(), after a user table has been updated. It seems that an updating user 
                  * transaction obtains an exclusive lock on the PUBLIC.SYS table.
                  */

                session = sessionHoldingExclusiveLock;
            }

            if (isLockedExclusivelyBy(session)) { return session; }

            if (noSessionHoldsExclusiveLock()) {

                if (exclusive) {

                    if (noSessionHoldsSharedLock()) {

                        traceLock(session, exclusive, "added for");
                        session.addLock(tableData);
                        recordExclusiveLock(session);
                        return session;
                    }
                    else if (sessionsHoldingSharedLocks.size() == 1 && isLockedSharedBy(session)) {

                        traceLock(session, exclusive, "add (upgraded) for ");
                        recordExclusiveLock(session);
                        return session;
                    }
                }

                else {

                    if (!isLockedSharedBy(session)) {

                        traceLock(session, exclusive, "ok");
                        session.addLock(tableData);
                        recordSharedLock(session);
                    }
                    return session;
                }
            }

            sessionsWaitingForLocks.put(session, tableData);

            if (checkDeadlock) {
                final ObjectArray sessions = checkDeadlock(session, null);
                if (sessions != null) { throw Message.getSQLException(ErrorCode.DEADLOCK_1, getDeadlockDetails(sessions)); }
            }
            else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }

            final long now = System.currentTimeMillis();
            if (now >= max) {
                traceLock(session, exclusive, "timeout after " + session.getLockTimeout());

                throw Message.getSQLException(ErrorCode.LOCK_TIMEOUT_1, tableData.getName());
            }
            try {
                traceLock(session, exclusive, "waiting for");
                if (database.getLockMode() == Constants.LOCK_MODE_TABLE_GC) {
                    for (int i = 0; i < 20; i++) {
                        final long free = Runtime.getRuntime().freeMemory();
                        System.gc();
                        final long free2 = Runtime.getRuntime().freeMemory();
                        if (free == free2) {
                            break;
                        }
                    }
                }
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK, max - now);
                if (sleep == 0) {
                    sleep = 1;
                }
                database.wait(sleep);
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }
    }

    private void traceLock(final Session session, final boolean exclusive, final String s) {

        if (traceLock.isDebugEnabled()) {
            traceLock.debug(session.getSessionId() + " " + (exclusive ? "exclusive write lock" : "shared read lock") + " " + s + " " + tableData.getName());
        }
    }

    String getDeadlockDetails(final ObjectArray sessions) {

        final StringBuilder buff = new StringBuilder();
        for (int i = 0; i < sessions.size(); i++) {
            buff.append('\n');
            final Session s = (Session) sessions.get(i);
            final Table lock = sessionsWaitingForLocks.get(s);
            buff.append("Session ").append(s).append(" is waiting to lock ").append(lock);
            buff.append(" while locking ");
            final Table[] locks = s.getLocks();
            for (int j = 0; j < locks.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                final Table t = locks[j];
                buff.append(t);
                if (t instanceof TableData) {
                    if (((TableData) t).isLockedExclusivelyBy(s)) {
                        buff.append(" (exclusive)");
                    }
                    else {
                        buff.append(" (shared)");
                    }
                }
            }
            buff.append('.');
        }
        return buff.toString();
    }

    public ObjectArray checkDeadlock(final Session session, Session clash) {

        // only one deadlock check at any given time
        synchronized (TableData.class) {
            if (clash == null) {
                // verification is started
                clash = session;
            }
            else if (clash == session) {
                // we found a circle
                return new ObjectArray();
            }

            ObjectArray error = null;
            for (final Session s : sessionsHoldingSharedLocks) {

                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                final Table t = sessionsWaitingForLocks.get(s);
                if (t != null) {
                    error = t.checkDeadlock(s, clash);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }

            if (error == null && sessionHoldingExclusiveLock != null) {
                final Table t = sessionsWaitingForLocks.get(sessionHoldingExclusiveLock);
                if (t != null) {
                    error = t.checkDeadlock(sessionHoldingExclusiveLock, clash);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }
}
