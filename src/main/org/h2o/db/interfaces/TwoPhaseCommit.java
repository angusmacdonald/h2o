/*
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
package org.h2o.db.interfaces;

import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * RMI interface for H2O's two phase commit functionality.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface TwoPhaseCommit {

	/**
	 * Prepare a query as per the two phase commit protocol. The query will be
	 * prepared on the given database instance, but will only be committed when
	 * the commit operation is called.
	 * 
	 * @param query
	 *            SQL query to be executed
	 * @param transactionName
	 *            The name to be given to this transaction - must be used again
	 *            to commit the transaction.
	 * @param commitOperation
	 *            True if this is a COMMIT, false if it is another type of
	 *            query. If it is false a PREPARE command will be executed to
	 *            get ready for the eventual commit.
	 * @return Result of the prepare - this should never fail in theory, bar
	 *         some weird disk-based mishap.
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public int execute(String query, String transactionName,
			boolean commitOperation) throws RemoteException, SQLException;

	/**
	 * Prepare the given machine to commit a set of queries that have already
	 * been executed.
	 * 
	 * @param transactionName
	 *            The name to be given to this transaction - must be used again
	 *            to commit the transaction.
	 * @return Result of the prepare - this should never fail in theory, bar
	 *         some weird disk-based mishap.
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public int prepare(String transactionName) throws RemoteException,
			SQLException;
}
