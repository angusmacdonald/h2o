/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2o.db.query.locking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Represents a locking table for a given table - this is maintained by the table's Table Manager.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LockingTable implements ILockingTable, Serializable {

    // TODO why does it need to be serializable?

    private static final long serialVersionUID = 2044915610751482232L;

    private LockRequest writeLockHolder;

    private final Set<LockRequest> readLockHolders;

    private final String tableName;
    private final String fullName;

    // Used for debugging.
    private static Map<String, LockingTable> lockTables = new HashMap<String, LockingTable>();
    private final StringBuilder lockHistory = new StringBuilder();

    public LockingTable(final String schemaName, final String tableName) {

        this.tableName = tableName;
        fullName = schemaName + "." + tableName;

        writeLockHolder = null;
        readLockHolders = new HashSet<LockRequest>();

        lockTables.put(tableName, this);
    }

    public static void dumpLockHistory(final String tableName) {

        final LockingTable lockingTable = lockTables.get(tableName);
        if (lockingTable == null) {
            System.out.println("no history for table: " + tableName);
        }
        else {
            lockingTable.dumpLockHistory();
        }
    }

    public void dumpLockHistory() {

        System.out.println("lock history for table: " + fullName);
        System.out.println(lockHistory);
    }

    @Override
    public synchronized LockType requestLock(final LockType lockType, final LockRequest lockRequest) {

        final LockType requestResult = requestLock2(lockType, lockRequest);
        lockHistory.append("\nREQUEST: lock type: " + lockType + "\n");
        lockHistory.append("lock request: " + lockRequest + "\n");
        lockHistory.append("result: " + requestResult + "\n");
        return requestResult;
    }

    private synchronized LockType requestLock2(final LockType lockType, final LockRequest lockRequest) {

        if (lockType == LockType.NONE) {

            // Just want replica locations.
            return LockType.NONE;
        }

        if (writeLockHolder != null) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock refused(1): " + lockType + " on " + fullName + " requester: " + lockRequest);

            // Exclusive lock already held by another session, so can't grant any type of lock.
            return LockType.NONE;
        }

        if (lockType == LockType.READ) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock granted: " + lockType + " on " + fullName + " requester: " + lockRequest);

            // At this point no exclusive lock is currently held, given previous check.
            // So the request can be granted.
            readLockHolders.add(lockRequest);
            return LockType.READ;
        }

        if ((lockType == LockType.WRITE || lockType == LockType.CREATE) && readLockHolders.size() == 0) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock granted: " + lockType + " on " + fullName + " requester: " + lockRequest);

            // This is a write lock request, and no read locks are currently held.

            writeLockHolder = lockRequest;
            return lockType; // Either WRITE or CREATE
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock refused(2): " + lockType + " on " + fullName + " requester: " + lockRequest);

        // Request is for a DROP lock, or for a WRITE/CREATE lock while there are current READ lock holders.
        // None of these can be granted.
        return LockType.NONE;
    }

    @Override
    public synchronized LockType releaseLock(final LockRequest lockRequest) {

        final LockType requestResult = releaseLock2(lockRequest);
        lockHistory.append("\nRELEASE:\n");
        lockHistory.append("lock request: " + lockRequest + "\n");
        lockHistory.append("result: " + requestResult + "\n");
        return requestResult;
    }

    private synchronized LockType releaseLock2(final LockRequest lockRequest) {

        if (readLockHolders.remove(lockRequest)) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock released: " + LockType.READ + " on " + fullName + " requester: " + lockRequest);
            return LockType.READ;
        }

        if (writeLockHolder != null && writeLockHolder.equals(lockRequest)) {

            writeLockHolder = null;
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock released: " + LockType.WRITE + " on " + fullName + " requester: " + lockRequest);
            return LockType.WRITE;
        }

        ErrorHandling.hardError("Unexpected Code Path: attempted to release a lock which wasn't held for table: " + fullName);
        return null; // Unreachable.
    }

    @Override
    public synchronized String toString() {

        return "LockingTable [writeLock=" + writeLockHolder + ", readLocksSize=" + readLockHolders.size() + "]";
    }

    @Override
    public synchronized LockType peekAtLockGranted(final LockRequest lockRequest) {

        if (readLockHolders.contains(lockRequest)) { return LockType.READ; }

        if (writeLockHolder != null && writeLockHolder.equals(lockRequest)) { return LockType.WRITE; }

        return LockType.NONE;
    }
}
