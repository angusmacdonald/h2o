package org.h2.h2o.comms;

import java.rmi.RemoteException;

import org.h2.command.Prepared;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface DatabaseInstanceRemote extends H2ORemote  {

	public int executeUpdate(String query) throws RemoteException;

}