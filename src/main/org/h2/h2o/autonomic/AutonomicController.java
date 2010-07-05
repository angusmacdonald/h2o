/*
 * Copyright 2009-2010 University of St Andrews. Licensed under the Eclipse Public License, Version 1.0
 * http://www.eclipse.org/legal/epl-v10.html
 * Initial Developer: University of St Andrews
 */
package org.h2.h2o.autonomic;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The components of the database system being monitored implement the AutonomicController class to provide a mechanism for autonomic
 * manager to update the functionality being managed.
 * 
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface AutonomicController extends Remote{
	
	/**
	 * Adjust some configuration of the component being controlled.
	 * @param action	The action (modification) to be performed.
	 * @return Whether the change was successfully applied.
	 */
	public boolean changeSetting(AutonomicAction action) throws RemoteException;
	
	
}
