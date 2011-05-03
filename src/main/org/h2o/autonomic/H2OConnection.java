package org.h2o.autonomic;

import java.util.Collection;
import java.util.List;
import java.util.Observable;

import uk.ac.standrews.cs.nds.events.Event;
import uk.ac.standrews.cs.nds.events.IEvent;

/**
 * Reporting class for H2O. Events are reported here by Numonic and the H2O instance which started Numonic
 * is an observer of this class. When the H2O instance receives an event notfication it is able to query
 * the public methods of this class to get monitoring data.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class H2OConnection extends Observable implements IReporting {

    IEvent NEW_RESOURCE_DATA_AVAILABLE = new Event("NEW_RESOURCE_DATA_AVAILABLE"); //monitoring of physical machine resources.
    IEvent NEW_NETWORK_DATA_AVAILABLE = new Event("NEW_NETWORK_DATA_AVAILABLE"); //monitoring of bandwidth/latency to other instances.
    IEvent SYSTEM_EVENT_UPDATE = new Event("SYSTEM_EVENT_UPDATE"); //system events include: machines restarting, ip change, numonic startup.

    //TODO temporary local storage of distribution data.

    private void notifyObservers(final String eventType) {

        setChanged();
        notifyObservers(eventType);
    }

    @Override
    public void reportDistributionData(final DistributionCollector<?> machineProbability) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportFileSystemData(final DistributionCollector<FileSystemData> fileSystemSummary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportFileSystemData(final MultipleSummary<FileSystemData> fileSystemSummary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportMachineUtilData(final SingleSummary<MachineUtilisationData> summary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportNetworkData(final SingleSummary<NetworkData> summary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportAllNetworkData(final MultipleSummary<NetworkData> allNetworkSummary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportProcessData(final SingleSummary<ProcessData> summary) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportAllProcessData(final MultipleSummary<ProcessData> summaries) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportSystemInfo(final SystemInfoData data) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportLatencyAndBandwidthData(final LatencyAndBandwidthData data) throws Exception {

        // TODO Auto-generated method stub

    }

    @Override
    public void reportEventData(final List<uk.ac.standrews.cs.numonic.event.Event> events) throws Exception {

        // TODO Auto-generated method stub

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

}
