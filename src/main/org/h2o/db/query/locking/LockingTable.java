/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.locking;

import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Represents a locking table for a given table - this is maintained by the table's Table Manager.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 * 
 */
public class LockingTable implements ILockingTable {
	
	private DatabaseInstanceWrapper writeLock;
	
	private Set<DatabaseInstanceWrapper> readLocks;
	
	private final String tableName;
	
	public LockingTable(String tableName) {
		this.tableName = tableName;
		this.writeLock = null;
		this.readLocks = new HashSet<DatabaseInstanceWrapper>();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.util.ILockingTable#requestLock(org.h2.h2o.util.LockType, org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	public synchronized LockType requestLock(LockType lockType, DatabaseInstanceWrapper requestingMachine) {
		if ( writeLock != null && !( requestingMachine.equals(writeLock) && lockType.equals(LockType.WRITE) ) ) {
			return LockType.NONE; // exclusive lock held.
		}
		
		if ( lockType == LockType.READ ) {
			
			readLocks.add(requestingMachine);
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "READ lock taken out on " + tableName);
			return LockType.READ;
			
		} else if ( ( lockType == LockType.WRITE || lockType == LockType.CREATE ) && ( readLocks.size() == 0 ) ) { // ||
																													// readLocks.contains(requestingMachine)
			// if write lock request + no read locks held.
			
			writeLock = requestingMachine;
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "WRITE lock taken out on " + tableName);
			return lockType; // will either be WRITE or CREATE
		}
		
		return LockType.NONE;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.h2o.util.ILockingTable#releaseLock(org.h2.h2o.comms.remote. DatabaseInstanceRemote)
	 */
	public synchronized LockType releaseLock(DatabaseInstanceWrapper requestingMachine) {
		LockType toReturn = LockType.NONE;
		// Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "'" + tableName +
		// "' unlocked by " + requestingMachine.getDatabaseURL().getOriginalURL());
		
		if ( readLocks.remove(requestingMachine) ) {
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "READ lock released on " + tableName);
			toReturn = LockType.READ;
		}
		
		if ( writeLock != null && writeLock.equals(requestingMachine) ) {
			writeLock = null;
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "WRITE lock released on " + tableName);
			toReturn = LockType.WRITE;
		}
		
		if ( !toReturn.equals(LockType.NONE) ) {
			return toReturn;
		}
		
		assert false : "Unexpected code path: attempted to release a lock which wasn't held for table: " + tableName;
		
		return toReturn; // should never get to this.
	}
	
	@Override
	public String toString() {
		return "LockingTable [writeLock=" + writeLock + ", readLocksSize=" + readLocks.size() + "]";
	}
	
}
