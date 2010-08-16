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
package org.h2o.autonomic.framework;

import java.rmi.Remote;
import java.rmi.RemoteException;


/**
 * <p>An autonomic monitor is responsible for monitoring some aspect of the database systems performance. For example,
 * a 'replication factor' monitor will constantly monitor the number of replicas in the system, and if there is a change in this
 * number it will produce an event which is sent to all register autonomic managers (see {@link AutonomicManager}).
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface AutonomicMonitor extends Remote {

	/**
	 * Called to subscribe to events from this monitor.
	 * 
	 * <p>Managers that subscribe to this monitor will recieve updates 
	 * 
	 * @param manager			The manager which is subscribing to this monitor.
	 * @param updateFrequency	The level of interest a manager has in this monitoring information. It may want to recieve updates every time there is a change,
	 * 	only at infrequent intervals, or somewhere in between.
	 */
	public void subscribe(AutonomicManager manager) throws RemoteException;

	/**
	 * Called to unsubscribe to events from this monitor.
	 * 
	 * @param manager	The manager which is unsubscribing from this monitor.
	 */
	public void unsubscribe(AutonomicManager manager) throws RemoteException;

	/**
	 * Request data on the state of the aspect being monitored. This provides a mechanism for synchronously
	 * acquiring such data, rather than using the publish-subscribe mechanism.
	 * @return
	 * @throws RemoteException
	 */
	public MonitoringData requestState() throws RemoteException;
}
