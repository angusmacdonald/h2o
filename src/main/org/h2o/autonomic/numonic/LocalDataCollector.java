package org.h2o.autonomic.numonic;

import org.h2o.autonomic.numonic.interfaces.ILocalDataCollector;
import org.h2o.autonomic.numonic.ranking.MachineMonitoringData;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.interfaces.ISystemTableReference;

import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

/**
 * This class collects incoming monitoring data on a local database instance, then sends it to the System Table when enough data has been collected.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LocalDataCollector implements ILocalDataCollector {

    /*
     * 
     * DATABASE STATE.
     * 
     */

    private final ISystemStatus sysStatus;

    /**
     * How this set of data will be identified when it is sent to the System Table.
     */
    private final DatabaseID localDatabaseID;

    /**
     * Where data will be sent.
     */
    private final ISystemTableReference systemTableRef;

    /*
     * 
     * MONITORING STATE.
     * 
     */

    /**
     * Used to temporarily store incoming machine utilization data before it can be combined with {@link #fsData} and
     * sent to the system table.
     */
    private MachineUtilisationData machineUtilData = null;

    /**
     * Used to temporarily store incoming file system monitoring data before it can be combined with {@link #machineUtilData} and
     * sent to the system table.
     */
    private FileSystemData fsData = null;

    private boolean sentMachineUtilData = false;
    private boolean sentFsData = false;

    /**
     * The number of measurements taken as part of each summary.
     */
    private int measurements_before_summary = 0;

    /**
     * Static information on the capacity of this machine.
     */
    private SystemInfoData staticSysInfoData;

    private boolean fsMonitoringEnabled;

    /**
     * @param localDatabaseID How this set of data will be identified when it is sent to the System Table.
     * @param systemTable Where data will be sent.
     */
    public LocalDataCollector(final DatabaseID localDatabaseID, final ISystemTableReference systemTable, final boolean fsMonitoringEnabled, final ISystemStatus sysStatus) {

        this.localDatabaseID = localDatabaseID;
        systemTableRef = systemTable;
        this.fsMonitoringEnabled = fsMonitoringEnabled;
        this.sysStatus = sysStatus;
    }

    /* (non-Javadoc)
     * @see org.h2o.autonomic.numonic.ILocalDataCollector#collateRankingData(uk.ac.standrews.cs.numonic.summary.SingleSummary)
     */
    @Override
    public void collateRankingData(final SingleSummary<? extends Data> summary) {

        if (summary.getMax() instanceof MachineUtilisationData) {
            machineUtilData = (MachineUtilisationData) summary.getAverage();
            sentMachineUtilData = false;
            measurements_before_summary = summary.getNumberOfMeasurements();
        }
        else if (summary.getMax() instanceof FileSystemData) {
            fsData = (FileSystemData) summary.getAverage();
            sentFsData = false;
        }

        if (staticSysInfoData == null) {
            ErrorHandling.error("No static system info has been received from Numonic.");
        }

        if (sysStatus.isConnected() && !sentMachineUtilData && machineUtilData != null && (!sentFsData || !fsMonitoringEnabled)) {
            sendDataToSystemTable(measurements_before_summary, staticSysInfoData, machineUtilData, fsData);

            sentMachineUtilData = true;
            sentFsData = true;
        }

    }

    @Override
    public void forceSendMonitoringData() {

        //        if (sysStatus.isConnected() && machineUtilData != null) {
        //            sendDataToSystemTable(measurements_before_summary, staticSysInfoData, machineUtilData, fsData);
        //        }
    }

    /**
     * Format monitoring data and send it to the System Table where machines can be ranked.
     * @param measurements_before_summary
     * @param machineUtilData
     * @param fsData
     */
    private void sendDataToSystemTable(final int measurements_before_summary, final SystemInfoData staticSysInfoData, final MachineUtilisationData machineUtilData, final FileSystemData fsData) {

        //Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Sending monitoring data from " + localDatabaseID + " to the System Table.");

        final MachineMonitoringData monitoringData = new MachineMonitoringData(localDatabaseID, staticSysInfoData, machineUtilData, fsData, measurements_before_summary);

        try {
            systemTableRef.getSystemTable().addMonitoringSummary(monitoringData);
        }
        catch (final Exception e) {
            ErrorHandling.errorNoEvent("Failed to send monitoring results to the System Table.");

        }
    }

    /* (non-Javadoc)
     * @see org.h2o.autonomic.numonic.ILocalDataCollector#setStaticSystemInfo(uk.ac.standrews.cs.numonic.data.SystemInfoData)
     */
    @Override
    public void setStaticSystemInfo(final SystemInfoData staticSysInfoData) {

        this.staticSysInfoData = staticSysInfoData;

    }

    @Override
    public void setFsMonitoringEnabled() {

        fsMonitoringEnabled = true;
    }

}
