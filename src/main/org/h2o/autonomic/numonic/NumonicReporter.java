package org.h2o.autonomic.numonic;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Observer;

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

    private final ThresholdChecker thresholdChecker;

    public NumonicReporter(final String numonicPropertiesFile, final Threshold... thresholds) {

        setName("numonic-reporting-thread");

        /*
         * Create Numonic instance and set up reporting class.
         */
        try {

            numonic = new Numonic(numonicPropertiesFile);

            numonic.setReporter(this);
        }
        catch (final UnknownHostException e) {
            e.printStackTrace();
        }

        thresholdChecker = new ThresholdChecker(thresholds);
    }

    public NumonicReporter(final String numonicPropertiesFile, final String thresholdPropertiesFile) throws IOException {

        this(numonicPropertiesFile, ThresholdChecker.getThresholds(thresholdPropertiesFile));
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

        //  System.out.println(fileSystemSummary);

    }

    @Override
    public void reportFileSystemData(final MultipleSummary<FileSystemData> fileSystemSummary) throws Exception {

        // System.out.println(fileSystemSummary);

    }

    @Override
    public void reportMachineUtilData(final SingleSummary<MachineUtilisationData> summary) throws Exception {

        thresholdChecker.analyseNewMonitoringData(summary);

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

        //System.out.println(PrettyPrinter.toString(events));

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
