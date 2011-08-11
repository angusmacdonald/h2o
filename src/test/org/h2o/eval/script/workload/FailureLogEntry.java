package org.h2o.eval.script.workload;

import java.text.DateFormat;

public class FailureLogEntry extends LogEntry {

    private final String failedMachineID;
    private final boolean startEvent;

    /**
     * @param timeOfFailure
     * @param failedMachineID
     * @param startEvent 
     */
    public FailureLogEntry(final long timeOfFailure, final String failedMachineID, final boolean startEvent) {

        super(timeOfFailure);

        this.failedMachineID = failedMachineID;
        this.startEvent = startEvent;
    }

    public String toCSV(final DateFormat dateformatter, final long startTime) {

        return super.toCSV(dateformatter, failedMachineID, null, startTime, null, true, 0, null, true, !startEvent);
    }

}
