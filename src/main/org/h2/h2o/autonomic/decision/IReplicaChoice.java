package org.h2.h2o.autonomic.decision;

import java.rmi.RemoteException;
import java.util.Queue;

import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.manager.MovedException;

public interface IReplicaChoice {
	/**
	 * Return an ordered set of machines on which data/procesess can be added to.
	 * @param typeOfRequest	Type of request being made. For example, a new replica being created, or a table manager being migrated.
	 * @throws MovedException 
	 * @throws RemoteException 
	 */
	public Queue<DatabaseInstanceWrapper> getAvailableMachines(RequestType typeOfRequest) throws RemoteException, MovedException;
}
