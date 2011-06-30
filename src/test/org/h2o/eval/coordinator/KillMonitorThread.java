package org.h2o.eval.coordinator;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.h2o.eval.Coordinator;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Thread which executes kill orders that are part of a co-ordinator scripts execution.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class KillMonitorThread extends Thread {

    private static int threadCount = 0;

    Map<Integer, Long> killOrders = Collections.synchronizedMap(new HashMap<Integer, Long>());
    private final Coordinator evaluationCoordinator;

    private boolean running = true;

    public KillMonitorThread(final Coordinator evaluationCoordinator) {

        setName("killMonitorThread" + threadCount++);

        this.evaluationCoordinator = evaluationCoordinator;
    }

    public void addKillOrder(final Integer instanceID, final Long killTime) {

        killOrders.put(instanceID, killTime);

    }

    @Override
    public void run() {

        while (isRunning()) {

            final List<Integer> toRemove = new LinkedList<Integer>();
            final long currentTimeMillis = System.currentTimeMillis();
            for (final Entry<Integer, Long> killOrder : killOrders.entrySet()) {

                if (killOrder.getValue() < currentTimeMillis) {
                    try {
                        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Activating kill switch!!!!!! ... on machine killOrder.getKey()");
                        evaluationCoordinator.killInstance(killOrder.getKey());
                        toRemove.add(killOrder.getKey());
                    }
                    catch (final Exception e) {
                        ErrorHandling.exceptionError(e, "Failed to kill instance with ID: " + killOrder.getKey());
                    }
                }
            }

            for (final Integer id : toRemove) {
                killOrders.remove(id);
            }

            toRemove.clear();

            try {
                Thread.sleep(1000);
            }
            catch (final InterruptedException e) {
            }
        }
    }

    public synchronized boolean isRunning() {

        return running;
    }

    public synchronized void setRunning(final boolean running) {

        this.running = running;
    }

}
