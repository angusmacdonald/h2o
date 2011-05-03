package org.h2o.autonomic;

import uk.ac.standrews.cs.numonic.appinterface.ResourceType;

/**
 * Represents a threshold that NUMONIC should be checking for. An instance of this class
 * specifies the resource that is being monitored, the level at which that resource goes beyond a threshold,
 * and whether the threshold is broken above or below this level.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class ThresholdConfiguration {

    /**
     * The resource this threshold applies to (e.g. CPU Utilization).
     */
    ResourceType resourceName;

    /**
     * The threshold level for this resource (e.g. 0.8, which in the context of CPU utilization may mean 80% utilization)
     */
    double value;

    /**
     * Whether the threshold is breached by utilization going above {@link #value} or below it. True for the breach occuring above the value; otherwise false.
     */
    boolean above;
}
