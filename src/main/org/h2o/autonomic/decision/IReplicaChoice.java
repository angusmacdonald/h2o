/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.autonomic.decision;

import java.rmi.RemoteException;
import java.util.Queue;

import org.h2o.autonomic.decision.ranker.metric.ActionRequest;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.util.exceptions.MovedException;

public interface IReplicaChoice {
	
	/**
	 * Return an ordered set of machines on which data/procesess can be added to.
	 * 
	 * @param typeOfRequest
	 *            Type of request being made. For example, a new replica being created, or a table manager being migrated.
	 * @throws MovedException
	 * @throws RemoteException
	 */
	public Queue<DatabaseInstanceWrapper> getAvailableMachines(ActionRequest typeOfRequest) throws RemoteException, MovedException;
}
