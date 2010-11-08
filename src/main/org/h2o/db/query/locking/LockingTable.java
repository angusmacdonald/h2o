/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.locking;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Represents a locking table for a given table - this is maintained by the table's Table Manager.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 * 
 */
public class LockingTable implements ILockingTable, Serializable {

    private static final long serialVersionUID = 2044915610751482232L;

    private LockRequest writeLock;

    private final Set<LockRequest> readLocks;

    private final String tableName;

    public LockingTable(final String tableName) {

        this.tableName = tableName;
        writeLock = null;
        readLocks = new HashSet<LockRequest>();
    }

    @Override
    public synchronized LockType requestLock(final LockType lockType, final LockRequest lockRequest) {

        if (writeLock != null) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock refused(1): " + lockType + " on " + tableName + " requester: " + lockRequest);
            // Exclusive lock already held by another session, so can't grant any type of lock.
            return LockType.NONE;
        }

        if (lockType == LockType.READ) {

            // At this point no exclusive lock is currently held, given previous check.
            // So the request can be granted.
            readLocks.add(lockRequest);
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock granted: " + lockType + " on " + tableName + " requester: " + lockRequest);
            return LockType.READ;
        }
        else if ((lockType == LockType.WRITE || lockType == LockType.CREATE) && readLocks.size() == 0) {

            // This is a write lock request, and no read locks are currently held.
            // TODO what about DROP requests?

            writeLock = lockRequest;
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock granted: " + lockType + " on " + tableName + " requester: " + lockRequest);
            return lockType; // Either WRITE or CREATE
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock refused(2): " + lockType + " on " + tableName + " requester: " + lockRequest);

        // Request is for a DROP lock, or for a WRITE/CREATE lock while there are current READ lock holders.
        // None of these can be granted.
        return LockType.NONE;
    }

    @Override
    public synchronized LockType releaseLock(final LockRequest lockRequest) {

        if (readLocks.remove(lockRequest)) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock released: " + LockType.READ + " on " + tableName + " requester: " + lockRequest);
            return LockType.READ;
        }

        if (writeLock != null && writeLock.equals(lockRequest)) {
            writeLock = null;
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "lock released: " + LockType.WRITE + " on " + tableName + " requester: " + lockRequest);
            return LockType.WRITE;
        }

        ErrorHandling.hardError("Unexpected Code Path: attempted to release a lock which wasn't held for table: " + tableName);
        return null; // Unreachable.
    }

    @Override
    public String toString() {

        return "LockingTable [writeLock=" + writeLock + ", readLocksSize=" + readLocks.size() + "]";
    }
}
