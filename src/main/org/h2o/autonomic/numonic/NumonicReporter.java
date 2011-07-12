package org.h2o.autonomic.numonic;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Observer;

import org.h2o.autonomic.numonic.interfaces.ILocalDataCollector;
import org.h2o.autonomic.numonic.interfaces.INumonic;
import org.h2o.autonomic.numonic.threshold.Threshold;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.interfaces.ISystemTableReference;

import uk.ac.standrews.cs.numonic.data.FileSystemData;
import uk.ac.standrews.cs.numonic.data.LatencyAndBandwidthData;
import uk.ac.standrews.cs.numonic.data.MachineUtilisationData;
import uk.ac.standrews.cs.numonic.data.NetworkData;
import uk.ac.standrews.cs.numonic.data.ProcessData;
import uk.ac.standrews.cs.numonic.data.SystemInfoData;
import uk.ac.standrews.cs.numonic.distribution.DistributionCollector;
import uk.ac.standrews.cs.numonic.event.Event;
import uk.ac.standrews.cs.numonic.main.Numonic;
import uk.ac.standrews.cs.numonic.reporting.IReporting;
import uk.ac.standrews.cs.numonic.sort.data.DistributionData;
import uk.ac.standrews.cs.numonic.summary.MultipleSummary;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;
import uk.ac.standrews.cs.numonic.util.IPropertiesWrapper;
import uk.ac.standrews.cs.numonic.util.JarPropertiesWrapper;

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
    private final ILocalDataCollector localDataCollector;

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
    public NumonicReporter(final String numonicPropertiesFileName, final String fileSystem, final DatabaseID localDatabaseID, final ISystemTableReference systemTable, final ISystemStatus db, final Threshold... thresholds) {

        setName("numonic-reporting-thread");

        /*
         * Create Numonic instance and set up reporting class.
         */
        try {

            if (numonicPropertiesFileName.startsWith("JAR:")) {
                final String location = numonicPropertiesFileName.substring("JAR:".length());
                //Get the properties file from inside the H2O jar file (note: this will still work in eclipse, etc.

                final File f = new File(location);

                if (f.exists()) {
                    numonic = new Numonic(location);
                }
                else {
                    final PropertiesFileLoader loader = new PropertiesFileLoader();
                    final IPropertiesWrapper propertiesWrapper = new JarPropertiesWrapper(loader.getResource(location));

                    propertiesWrapper.loadProperties();

                    numonic = new Numonic(propertiesWrapper);
                }
            }
            else {
                numonic = new Numonic(numonicPropertiesFileName);
            }

            numonic.setReporter(this);
        }
        catch (final IOException e) {
            e.printStackTrace();
        }

        this.fileSystem = fileSystem;

        thresholdChecker = new ThresholdChecker(thresholds);
        localDataCollector = new LocalDataCollector(localDatabaseID, systemTable, false, db);
    }

    public NumonicReporter(final String numonicPropertiesFile, final String fileSystem, final DatabaseID localDatabaseID, final ISystemTableReference systemTable, final ISystemStatus db, final String thresholdPropertiesFile) throws IOException {

        this(numonicPropertiesFile, fileSystem, localDatabaseID, systemTable, db, ThresholdChecker.getThresholds(thresholdPropertiesFile));
    }

    /**
     * Start numonic's monitoring activities.
     */
    @Override
    public void run() {

        numonic.start();
    }

    @Override
    public boolean reportFileSystemData(final MultipleSummary<FileSystemData> fileSystemSummary) throws Exception {

        /*
         * Only check for thresholds on the file system being used by the database system, and not any others.
         */
        if (fileSystem != null) {
            for (final SingleSummary<FileSystemData> specificFsSummary : fileSystemSummary.getSummaries()) {
                if (thisIsPrimaryFileSystem(specificFsSummary)) {
                    localDataCollector.setFsMonitoringEnabled();
                    collectFileSystemData(specificFsSummary);
                    break;
                }
            }
        }

        return true;

    }

    /**
     * Collect data from this file system summary and send it to the threshold checker and to the resource ranker.
     * @param specificFsSummary  Summary of file system utilization
     */
    public void collectFileSystemData(final SingleSummary<FileSystemData> specificFsSummary) {

        thresholdChecker.analyseNewMonitoringData(specificFsSummary);
        localDataCollector.collateRankingData(specificFsSummary);
    }

    /**
     * 
     * @param specificFsSummary Summary of file system utilization, including the name of the filesystem.
     * @return true if this is data for the primary file system, as specified by {@link #fileSystem}.
     */
    public boolean thisIsPrimaryFileSystem(final SingleSummary<FileSystemData> specificFsSummary) {

        return specificFsSummary.getMax().file_system_location.equalsIgnoreCase(fileSystem);
    }

    @Override
    public boolean reportMachineUtilData(final SingleSummary<MachineUtilisationData> summary) throws Exception {

        thresholdChecker.analyseNewMonitoringData(summary);
        localDataCollector.collateRankingData(summary);

        return true;
    }

    @Override
    public boolean reportSystemInfo(final SystemInfoData staticSysInfoData) throws Exception {

        localDataCollector.setStaticSystemInfo(staticSysInfoData);

        return true;
    }

    /* (non-Javadoc)
     * @see org.h2o.autonomic.numonic.INumonic#addObserver(java.util.Observer)
     */
    @Override
    public void addObserver(final Observer observer) {

        thresholdChecker.addObserver(observer);
    }

    @Override
    public void forceSendMonitoringData() {

        //Start call in new thread to prevent blocking on other operations that were taking place while the System
        //Table's location was changed (e.g. queries).

        class ForceThread extends Thread {

            @Override
            public void run() {

                localDataCollector.forceSendMonitoringData();
            }
        };

        final ForceThread force = new ForceThread();
        force.start();

    }

    @Override
    public void shutdown() {

        numonic.setRunning(false);
    }

    @Override
    public boolean reportDistributionData(final DistributionCollector<?> machineProbability) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportNetworkData(final SingleSummary<NetworkData> summary) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportAllNetworkData(final MultipleSummary<NetworkData> allNetworkSummary) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportProcessData(final SingleSummary<ProcessData> summary) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportAllProcessData(final MultipleSummary<ProcessData> summaries) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportLatencyAndBandwidthData(final LatencyAndBandwidthData data) throws Exception {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean reportEventData(final List<Event> events) throws Exception {

        // TODO Auto-generated method stub
        return false;
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
    public Collection<Event> getEvents(final int minutes) throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setMachineID(final String machineID) {

        // TODO Auto-generated method stub

    }

    @Override
    public List<String> getActiveKnownHosts() throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<SystemInfoData> getSystemInfo() throws Exception {

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setHostPassive(final String hostname) throws SQLException {

        // TODO Auto-generated method stub

    }

    @Override
    public boolean setHostActive() throws SQLException {

        // TODO Auto-generated method stub
        return false;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Methods below are not implemented.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

}
