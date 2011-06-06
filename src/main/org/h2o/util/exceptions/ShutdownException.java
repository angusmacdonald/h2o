package org.h2o.util.exceptions;

/**
 * Exception when trying to shut down a particular component.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class ShutdownException extends Exception {

    public ShutdownException(final String message) {

        super(message);
    }

    /**
     * 
     */
    private static final long serialVersionUID = -6402290537689635386L;

}
