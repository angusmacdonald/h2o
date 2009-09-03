package org.h2.h2o.comms;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.h2.command.Command;
import org.h2.command.Prepared;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface IDataManagerRemote extends Remote {

	public void requestLock(String message) throws RemoteException;
}
