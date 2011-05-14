package org.h2o.test.fixture;

import java.util.Map;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class MultiProcessCloser extends Thread {

    private final Map<String, Process> activeProcesses;

    public MultiProcessCloser(final Map<String, Process> processes) {

        activeProcesses = processes;

    }

    @Override
    public void run() {

        for (final Process p : activeProcesses.values()) {
            try {
                if (p != null) {

                    p.destroy();
                }
            }
            catch (final Exception e) {
                ErrorHandling.errorNoEvent("Error trying to terminate a process via the shutdown hook.");
            }
        }

    }

}
