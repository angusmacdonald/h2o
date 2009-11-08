package org.h2.h2o.locking;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.LockType;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * Represents a locking table for a given table - this is maintained by the table's data manager.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 *
 */
public class LockingTable implements ILockingTable {

	private DatabaseInstanceRemote writeLock;
	private Set<DatabaseInstanceRemote> readLocks;

	private String tableName; //used entirely for trace output in this class.

	public LockingTable(String tableName){
		this.tableName = tableName;
		this.writeLock = null;
		this.readLocks = new HashSet<DatabaseInstanceRemote>();
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.util.ILockingTable#requestLock(org.h2.h2o.util.LockType, org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	public synchronized LockType requestLock(LockType lockType, DatabaseInstanceRemote requestingMachine){

		if (writeLock != null) return LockType.NONE; //exclusive lock held.

		if ((lockType == LockType.WRITE || lockType == LockType.CREATE) && readLocks.size() == 0){
			//if write lock request + no read locks held.

			try {
				Diagnostic.traceNoEvent(Diagnostic.FULL, "'" + tableName + "' " + ((LockType.CREATE == lockType)? "CREATE": "WRITE") + " locked by: " + requestingMachine.getConnectionString());
			} catch (RemoteException e) {}

			writeLock = requestingMachine;
			return lockType; //will either be WRITE or CREATE
		} else if (lockType == LockType.READ){

			try {
				Diagnostic.traceNoEvent(Diagnostic.FULL, "'" + tableName + "' READ locked by: " + requestingMachine.getConnectionString());
			} catch (RemoteException e) {}

			readLocks.add(requestingMachine);
			return LockType.READ;
		}

		return LockType.NONE;
	}

	/* (non-Javadoc)
	 * @see org.h2.h2o.util.ILockingTable#releaseLock(org.h2.h2o.comms.remote.DatabaseInstanceRemote)
	 */
	public synchronized boolean releaseLock(DatabaseInstanceRemote requestingMachine){
		try {
			Diagnostic.traceNoEvent(Diagnostic.FULL, "'" + tableName + "' unlocked by " + requestingMachine.getConnectionString());
		} catch (RemoteException e) {}


		if (writeLock != null && writeLock.equals(requestingMachine)){
			writeLock = null;
			return true;
		} else if (readLocks.contains(requestingMachine)){
			readLocks.remove(requestingMachine);
			return true;
		}

		//ErrorHandling.hardError("UNEXPECTED CODE PATH: FAILED TO RELEASE LOCK.");
		return false;
	}

}
