package org.h2.h2o.locking;

import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.util.LockType;

/**
 * Interface for a table lock manager. Each manager controls access to a single table. Calling classes can either
 * request a lock or release a currently held lock.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface ILockingTable {

	/**
	 * Request a lock on the given table.
	 * @param lockType	Type of lock requested.
	 * @param requestingMachine Proxy for the machine making the request.
	 * @return			Type of lock granted.
	 */
	public LockType requestLock(LockType lockType,
			DatabaseInstanceRemote requestingMachine);

	/**
	 * Release the lock of this type held by this machine.
	 * @param lockType	Type of lock held.
	 * @param requestingMachine Proxy for the machine making the request.
	 * @return True if the success was successful.
	 */
	public boolean releaseLock(DatabaseInstanceRemote requestingMachine);

}