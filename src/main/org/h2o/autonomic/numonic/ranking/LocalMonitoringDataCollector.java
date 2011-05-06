package org.h2o.autonomic.numonic.ranking;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

/**
 * This class collects incoming monitoring data, then sends it to the System Table when enough data has been collected.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LocalMonitoringDataCollector {

    /*
     * 
     * DATABASE STATE.
     * 
     */

    /**
     * How this set of data will be identified when it is sent to the System Table.
     */
    private final DatabaseID localDatabaseID;

    /**
     * Where data will be sent.
     */
    private final ISystemTableReference systemTable;

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

    /**
     * The number of measurements taken as part of each summary.
     */
    private int measurements_before_summary = 0;

    /**
     * @param localDatabaseID How this set of data will be identified when it is sent to the System Table.
     * @param systemTable Where data will be sent.
     */
    public LocalMonitoringDataCollector(final DatabaseID localDatabaseID, final ISystemTableReference systemTable) {

        this.localDatabaseID = localDatabaseID;
        this.systemTable = systemTable;
    }

    /**
     * Wait for ranking data to be received for both file system data and for CPU and memory data, then send it to the
     * System Table.
     * @param summary New monitoring data.
     */
    public void collateRankingData(final SingleSummary<? extends Data> summary) {

        if (machineUtilData == null && summary.getMax() instanceof MachineUtilisationData) {
            machineUtilData = (MachineUtilisationData) summary.getAverage();
            measurements_before_summary = summary.getNumberOfMeasurements();
        }
        else if (fsData == null && summary.getMax() instanceof FileSystemData) {
            fsData = (FileSystemData) summary.getAverage();
        }

        if (machineUtilData != null && fsData != null) {
            sendDataToSystemTable(measurements_before_summary, machineUtilData, fsData);

            machineUtilData = null;
            fsData = null;
        }

    }

    /**
     * Format monitoring data and send it to the System Table where machines can be ranked.
     * @param measurements_before_summary
     * @param machineUtilData
     * @param fsData
     */
    private void sendDataToSystemTable(final int measurements_before_summary, final MachineUtilisationData machineUtilData, final FileSystemData fsData) {

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Sending monitoring data from " + localDatabaseID + " to the System Table.");

        final MachineMonitoringData monitoringData = new MachineMonitoringData(localDatabaseID, machineUtilData, fsData, measurements_before_summary);

        try {
            systemTable.getSystemTable().addMonitoringSummary(monitoringData);
        }
        catch (final RPCException e) {
            ErrorHandling.exceptionError(e, "Failed to send monitoring results to the System Table.");
        }
        catch (final MovedException e) {
            ErrorHandling.exceptionError(e, "Failed to send monitoring results to the System Table.");
            //TODO send to new location.
        }
    }

}
