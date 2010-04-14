package org.h2.h2o.autonomic;

/**
 * Instances of monitoring data store data describing the current state of some aspect of the database system.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public interface MonitoringData {
	/**
	 * Specifies where the monitoring data originated.
	 * @return Location of monitoring data (e.g. Table Manager, Database Instance, Local Resource Monitoring)
	 */
	public MonitoringDataLocation getDataLocation();
	
	/**
	 * Specifies precisely what is being monitored.
	 * @return	
	 */
	public String getAspectBeingMonitored();
	
	/**
	 * The results of the monitoring.
	 * @return
	 */
	public Object getData();
}
