package org.h2o.util.exceptions;

/**
 * Thrown when there has been a problem parsing a line from a workload script.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class WorkloadParseException extends Exception {

    /**
     * 
     */
    private static final long serialVersionUID = 8665881713901101317L;

    public WorkloadParseException(final String message) {

        super(message);
    }
}
