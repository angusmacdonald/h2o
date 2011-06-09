package org.h2o.eval.coordinator;

import java.io.IOException;

import org.h2o.util.exceptions.StartupException;

/**
 * Interface for issuing commands to the evaluation coordinator.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public interface ICoordinatorLocal {

    public void startLocatorServer(int locatorPort) throws IOException;

    /**
     * Start H2O instances at the specified locations.
     * @param numberToStart The number of H2O instances to start.
     * @return the number of instances that were successfully started.
     * @throws StartupException Thrown if the instances couldn't be started because the {@link #startLocatorServer(int)} method
     * has not yet been called.
     */
    public int startH2OInstances(int numberToStart) throws StartupException;

}
