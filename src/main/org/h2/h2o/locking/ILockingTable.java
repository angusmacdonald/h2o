﻿/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2.h2o.locking;

import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
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
	 * @param databaseInstanceWrapper Proxy for the machine making the request.
	 * @return			Type of lock granted.
	 */
	public LockType requestLock(LockType lockType,
			DatabaseInstanceWrapper databaseInstanceWrapper);

	/**
	 * Release the lock of this type held by this machine.
	 * @param lockType	Type of lock held.
	 * @param requestingMachine Proxy for the machine making the request.
	 * @return True if the success was successful.
	 */
	public boolean releaseLock(DatabaseInstanceWrapper requestingMachine);

}
