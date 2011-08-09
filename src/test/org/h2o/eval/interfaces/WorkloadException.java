package org.h2o.eval.interfaces;

import org.h2o.eval.script.workload.Workload;

/**
 * Represents an error created by a {@link Workload}'s execution.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class WorkloadException extends Exception {

    private static final long serialVersionUID = -5122509844010325240L;

    public WorkloadException(final String message) {

        super(message);
    }
}
