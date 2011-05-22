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
     * Stop the numonic monitor.
     */
    public void shutdown();

}
