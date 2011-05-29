package org.h2o.autonomic.numonic.interfaces;

import java.util.Observer;

/**
 * Interface for the Numonic monitoring instance attached to H2O. Allows the H2O application to start monitoring,
 * and to subscribe to monitoring events.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public interface INumonic {

    /**
     * Start the numonic monitor.
     */
    public void start();

    /**
     * Add a class as an observer to the threshold monitoring instance for numonic.
     * @param observer Class wanting to observe numonic threshold results.
     */
    public abstract void addObserver(final Observer observer);

    /**
     * Tell the monitor to immediately send monitoring data to the central collector (e.g. the System Table), rather than wait for
     * new monitoring data to become available.
     * 
     * <p>This operation is performed in a new thread to stop this call blocking other operations.
     */
    public void forceSendMonitoringData();

    /**
     * Stop the numonic monitor.
     */
    public void shutdown();

}
