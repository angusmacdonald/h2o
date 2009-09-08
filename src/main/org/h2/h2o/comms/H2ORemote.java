package org.h2.h2o.comms;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Top-level remote interface for H2O objects. Specifies methods common to them all.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface H2ORemote extends Remote {

	/**
	 * Used to check that a data manager is still accessible via RMI. This method shouldn't do anything -
	 * an exception will be thrown if it is unavailable.
	 */
	public void testAvailability() throws RemoteException;
}
