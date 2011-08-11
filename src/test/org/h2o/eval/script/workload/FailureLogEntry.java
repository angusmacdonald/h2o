package org.h2o.eval.script.workload;

import java.text.DateFormat;

public class FailureLogEntry extends LogEntry {

    private final String failedMachineID;

    /**
     * @param timeOfFailure
     * @param failedMachineID
     */
    public FailureLogEntry(final long timeOfFailure, final String failedMachineID) {

        super(timeOfFailure);

        this.failedMachineID = failedMachineID;
    }

    public String toCSV(final DateFormat dateformatter, final long startTime) {

        return super.toCSV(dateformatter, failedMachineID, null, startTime, null, true, 0, null, true);
    }

}
