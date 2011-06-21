package org.h2o.util.exceptions;

/**
 * Thrown when there has been a problem parsing a line from a workload script.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class WorkloadParseException extends Exception {

    public WorkloadParseException(final String message) {

        super(message);
    }

    /**
     * 
     */
    private static final long serialVersionUID = 1165496111325039817L;

}
