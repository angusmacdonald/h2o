package org.h2.h2o.autonomic;

import java.io.Serializable;
import java.util.Observer;

/**
 * <p>An autonomic manager is responsible for some aspect of the database system's operation, such as replication factor.
 * 
 * <p>The manager must register an interest in monitoring events (see {@link AutonomicMonitor}) relevant to its decision making. For example,
 *  a manager responsible for controlling replication factor must register for all events that may require a change in the current replication factor.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface AutonomicManager extends Serializable, Observer {
	/**
	 * Sends monitoring data to the manager so that the knowledge-base can be updated and analysed. This method
	 * is called by classes implementing {@link AutonomicMonitor} where the manager has subscribed for updates from the given class. 
	 * @param monitoringData	Data which provides 
	 */
	public void recieveData(MonitoringData monitoringData);
}
