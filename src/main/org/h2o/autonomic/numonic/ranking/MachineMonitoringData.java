package org.h2o.autonomic.numonic.ranking;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;

public class MachineMonitoringData {

    /**
     * The database which this monitoring data is taken from.
     */
    private final DatabaseID databaseID;

    //Null at first but may be set by the system table when it is received.
    private DatabaseInstanceWrapper databaseWrapper = null;

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

    private final SystemInfoData staticSystemData;

    public MachineMonitoringData(final DatabaseID localDatabaseID, final SystemInfoData staticSysInfoData, final MachineUtilisationData machineUtilData, final FileSystemData fsData, final int measurements_before_summary) {

        databaseID = localDatabaseID;
        staticSystemData = staticSysInfoData;
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

    public SystemInfoData getSystemInfoData() {

        return staticSystemData;
    }

    @Override
    public String toString() {

        return "MachineMonitoringData [databaseID=" + databaseID + ", \n\tmachineUtilData=" + machineUtilData + ", \n\tfsData=" + fsData + ", \n\tmeasurements_before_summary=" + measurements_before_summary + ", \n\tstaticSysInfoData=" + staticSystemData + "\n]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (databaseID == null ? 0 : databaseID.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final MachineMonitoringData other = (MachineMonitoringData) obj;
        if (databaseID == null) {
            if (other.databaseID != null) { return false; }
        }
        else if (!databaseID.equals(other.databaseID)) { return false; }
        return true;
    }

    public boolean meetsRequirements(final Requirements requirements) {

        return (staticSystemData == null || requirements.getCpuCapacity() > staticSystemData.getCpuClockSpeed()) && (staticSystemData == null || requirements.getMemoryCapacity() > staticSystemData.getMemoryTotal()) && (fsData == null || requirements.getDiskCapacity() > fsData.getSpaceFree());

    }

    public DatabaseInstanceWrapper getDatabaseWrapper() {

        return databaseWrapper;
    }

    public void setDatabaseWrapper(final DatabaseInstanceWrapper wrapper) {

        databaseWrapper = wrapper;
    }

}
