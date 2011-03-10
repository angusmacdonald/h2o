package org.h2o.db.manager.monitoring.systemtable;

import java.util.List;

import org.h2o.autonomic.framework.MonitoringData;
import org.h2o.db.id.DatabaseID;

/**
 * Value class giving a summary of the available resources on a specific database instance.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class InstanceResourceSummary {

    /**
     * The database instance that this summary comes from.
     */
    private final DatabaseID databaseID;

    /**
     * The Time that the monitoring information used in this summary started being collected.
     */
    private final long monitoringStartTime;

    /**
     * The Time that the monitoring information used in this summary stopped being collected.
     */
    private final long monitoringEndTime;

    /**
     * The number of samples that make up this summary.
     */
    private final int numberOfSamples;

    private final List<MonitoringData> samples;

    /**
     * @param databaseID
     * @param monitoringStartTime
     * @param monitoringEndTime
     * @param numberOfSamples
     * @param samples
     */
    public InstanceResourceSummary(final DatabaseID databaseID, final long monitoringStartTime, final long monitoringEndTime, final int numberOfSamples, final List<MonitoringData> samples) {

        this.databaseID = databaseID;
        this.monitoringStartTime = monitoringStartTime;
        this.monitoringEndTime = monitoringEndTime;
        this.numberOfSamples = numberOfSamples;
        this.samples = samples;
    }

    public DatabaseID getDatabaseID() {

        return databaseID;
    }

    public long getMonitoringStartTime() {

        return monitoringStartTime;
    }

    public long getMonitoringEndTime() {

        return monitoringEndTime;
    }

    public int getNumberOfSamples() {

        return numberOfSamples;
    }

    public List<MonitoringData> getSamples() {

        return samples;
    }

    /*
     * NOTE: Equality comparisons current include the start time of monitoring, to make each summary unique. Depending on the final
     * use case of this class it may be simpler to just use the Database ID for equality comparisons.
     */
    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (databaseID == null ? 0 : databaseID.hashCode());
        result = prime * result + (int) (monitoringStartTime ^ monitoringStartTime >>> 32);
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final InstanceResourceSummary other = (InstanceResourceSummary) obj;
        if (databaseID == null) {
            if (other.databaseID != null) { return false; }
        }
        else if (!databaseID.equals(other.databaseID)) { return false; }
        if (monitoringStartTime != other.monitoringStartTime) { return false; }
        return true;
    }

}
