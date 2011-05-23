package org.h2o.autonomic.numonic.metric;

import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;

/**
 * Metric which defines how {@link MachineMonitoringData} instances should be sorted.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface IMetric {

    /**
     * @return the importance of CPU utilization to the requesting application, between 0-1
     */
    public double getCpuUtilization();

    /**
    * @return the importance of memory utilization to the requesting application, between 0-1
     */
    public double getMemoryUtilization();

    /**
    * @return the importance of swap space utilization to the requesting application, between 0-1
     */
    public double getSwapUtilization();

    /**
    * @return the importance of disk utilization reads to the requesting application, between 0-1
     */
    public double getDiskUtilizationRead();

    /**
     * @return the importance of disk utilization writes to the requesting application, between 0-1
     */
    public double getDiskUtilizationWrite();

    /**
    * @return the importance of network utilization reads to the requesting application, between 0-1
     */
    public double getNetworkUtilizationRead();

    /**
     * @return the importance of network utilization writes to the requesting application, between 0-1
     */
    public double getNetworkUtilizationWrite();

}
