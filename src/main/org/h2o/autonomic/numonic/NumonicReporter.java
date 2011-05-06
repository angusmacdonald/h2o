package org.h2o.autonomic.numonic;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Observer;

import org.h2o.autonomic.numonic.ranking.ResourceRanker;
import org.h2o.autonomic.numonic.threshold.Threshold;
import org.h2o.autonomic.numonic.threshold.ThresholdChecker;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.interfaces.ISystemTableReference;

import uk.ac.standrews.cs.nds.util.PrettyPrinter;
import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.LatencyAndBandwidthData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.NetworkData;
import uk.ac.standrews.cs.numonic.data.ProcessData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;
import uk.ac.standrews.cs.numonic.distribution.DistributionCollector;
import uk.ac.standrews.cs.numonic.main.Numonic;
import uk.ac.standrews.cs.numonic.reporting.IReporting;
import uk.ac.standrews.cs.numonic.sort.data.DistributionData;
import uk.ac.standrews.cs.numonic.summary.MultipleSummary;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

/**
 * Reporting class for H2O. Events are reported here by Numonic and the H2O instance which started Numonic
 * is an observer of this class. When the H2O instance receives an event notfication it is able to query
 * the public methods of this class to get monitoring data.
 * 
 * <p>To start monitoring on numonic, create a new instance of this class and call the {@link #start()} method.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class NumonicReporter extends Thread implements IReporting, INumonic {

    Numonic numonic = null;

    /**
     * Checks incoming monitoring data to see whether it has broken any thresholds.
     */
    private final ThresholdChecker thresholdChecker;

    /**
     * Collects monitoring data and sends it to the System Table when enough has been collected.
     */
    private final ResourceRanker resourceRanker;

    /**
     * The name of the file system the database is running on. For example, "C:\" on windows.
     */
    private final String fileSystem;

    /**
     * 
     * @param numonicPropertiesFileName Path to the configuration file needed to start numonic. The default file is called default_numonic_configuration.properties.
     * @param fileSystem    The name of the file system the database is running on. For example, "C:\" on windows.
     * @param localDatabaseID ID of the local database instance. Used to identify where data has come from when it is sent remotely.
     * @param systemTable Reference to the System Table wrapper class. Used to send data to the system table when enough has been collected.
     * @param thresholds    Array of thresholds that the system must check for.
     */
    public NumonicReporter(final String numonicPropertiesFileName, final String fileSystem, final DatabaseID localDatabaseID, final ISystemTableReference systemTable, final Threshold... thresholds) {

        setName("numonic-reporting-thread");

        /*
         * Create Numonic instance and set up reporting class.
         */
        try {

            numonic = new Numonic(numonicPropertiesFileName);

            numonic.setReporter(this);
        }
        catch (final UnknownHostException e) {
            e.printStackTrace();
        }

        this.fileSystem = fileSystem;

        thresholdChecker = new ThresholdChecker(thresholds);
        resourceRanker = new ResourceRanker(localDatabaseID, systemTable);
    }

    public NumonicReporter(final String numonicPropertiesFile, final String fileSystem, final DatabaseID localDatabaseID, final ISystemTableReference systemTable, final String thresholdPropertiesFile) throws IOException {

        this(numonicPropertiesFile, fileSystem, localDatabaseID, systemTable, ThresholdChecker.getThresholds(thresholdPropertiesFile));
    }

    /**
     * Start numonic's monitoring activities.
     */
    @Override
    public void run() {

        numonic.start();
    }

    @Override
    public void reportDistributionData(final DistributionCollector<?> machineProbability) throws Exception {

        // System.out.println(machineProbability);

    }

    @Override
    public void reportFileSystemData(final DistributionCollector<FileSystemData> fileSystemSummary) throws Exception {

        //System.out.println(fileSystemSummary);

    }

    @Override
    public void reportFileSystemData(final MultipleSummary<FileSystemData> fileSystemSummary) throws Exception {

        /*
         * Only check for thresholds on the file system being used by the database system, and not any others.
         */

        for (final SingleSummary<FileSystemData> specificFsSummary : fileSystemSummary.getSummaries()) {
            if (specificFsSummary.getMax().file_system_location.equalsIgnoreCase(fileSystem)) {
                thresholdChecker.analyseNewMonitoringData(specificFsSummary);
                resourceRanker.collateRankingData(specificFsSummary);
                break;
            }

        }

    }

    @Override
    public void reportMachineUtilData(final SingleSummary<MachineUtilisationData> summary) throws Exception {

        thresholdChecker.analyseNewMonitoringData(summary);
        resourceRanker.collateRankingData(summary);
    }

    @Override
    public void reportNetworkData(final SingleSummary<NetworkData> summary) throws Exception {

        // System.out.println(summary);

    }

    @Override
    public void reportAllNetworkData(final MultipleSummary<NetworkData> allNetworkSummary) throws Exception {

        // System.out.println(allNetworkSummary);

    }

    @Override
    public void reportProcessData(final SingleSummary<ProcessData> summary) throws Exception {

        // System.out.println(summary);

    }

    @Override
    public void reportAllProcessData(final MultipleSummary<ProcessData> summaries) throws Exception {

        //System.out.println(summaries);

    }

    @Override
    public void reportSystemInfo(final SystemInfoData data) throws Exception {

        //System.out.println(data);

    }

    @Override
    public void reportLatencyAndBandwidthData(final LatencyAndBandwidthData data) throws Exception {

        //System.out.println(data);

    }

    @Override
    public void reportEventData(final List<uk.ac.standrews.cs.numonic.event.Event> events) throws Exception {

        System.out.println(PrettyPrinter.toString(events));

    }

    @Override
    public List<FileSystemData> getFileSystemData(final int minutes) throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<DistributionData> getDistributionData(final int minutes) throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<uk.ac.standrews.cs.numonic.event.Event> getEvents(final int minutes) throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.h2o.autonomic.numonic.INumonic#addObserver(java.util.Observer)
     */
    @Override
    public void addObserver(final Observer observer) {

        thresholdChecker.addObserver(observer);
    }

}
