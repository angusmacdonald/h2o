package org.h2o.autonomic.numonic.ranking;

import org.h2o.db.id.DatabaseID;

import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;

public class MachineMonitoringData {

    /**
     * The database which this monitoring data is taken from.
     */
    private final DatabaseID databaseID;

    /**
     * Average of CPU and memory resources on the machine.
     */
    private final MachineUtilisationData machineUtilData;

    /**
     * Average file system utilization on the machine.
     */
    private final FileSystemData fsData;

    /**
     * The number of monitoring samples taken as part of this average.
     */
    private final int measurements_before_summary;

    public MachineMonitoringData(final DatabaseID localDatabaseID, final MachineUtilisationData machineUtilData, final FileSystemData fsData, final int measurements_before_summary) {

        databaseID = localDatabaseID;
        this.machineUtilData = machineUtilData;
        this.fsData = fsData;
        this.measurements_before_summary = measurements_before_summary;

    }

    public DatabaseID getDatabaseID() {

        return databaseID;
    }

    public MachineUtilisationData getMachineUtilData() {

        return machineUtilData;
    }

    public FileSystemData getFsData() {

        return fsData;
    }

    public int getMeasurementsBeforeSummary() {

        return measurements_before_summary;
    }

    @Override
    public String toString() {

        return "MachineMonitoringData [databaseID=" + databaseID + ", machineUtilData=" + machineUtilData + ", fsData=" + fsData + ", measurements_before_summary=" + measurements_before_summary + "]";
    }

}
