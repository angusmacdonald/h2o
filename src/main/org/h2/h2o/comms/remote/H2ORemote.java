package org.h2.h2o.comms.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.h2.h2o.manager.MovedException;

/**
 * Top-level remote interface for H2O objects. Specifies methods common to them all.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface H2ORemote extends Remote {

	/**
	 * Used to check that a data manager is still accessible via RMI. This method will return false if the database has been closed -
	 * an exception will be thrown if it is unavailable.
	 * @throws MovedException 
	 */
	public boolean isAlive() throws RemoteException, MovedException;
}
