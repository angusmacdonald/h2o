package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.h2o.eval.Coordinator;
import org.h2o.eval.coordinator.KillMonitorThread;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.eval.script.workload.FailureLogEntry;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;

public class CoordinatorScriptState {

    private final Coordinator coord;

    private final Map<Integer, IWorker> scriptedInstances = new HashMap<Integer, IWorker>();

    private final List<FailureLogEntry> failureLog = new LinkedList<FailureLogEntry>();

    private KillMonitorThread killMonitor;

    private long currentExecutionTime;

    private boolean startedExecution = false;

    public CoordinatorScriptState(final Coordinator coord) {

        this.coord = coord;

    }

    public IWorker getScriptedInstance(final Integer id_int) {

        return scriptedInstances.get(id_int);
    }

    public void addScriptedInstance(final Integer id_int, final IWorker worker) {

        scriptedInstances.put(id_int, worker);
    }

    public void blockWorkloads() throws RemoteException, WorkloadException {

        for (final IWorker runningWorker : scriptedInstances.values()) {
            runningWorker.stallWorkloads();
        }
    }

    public void resumeWorkloads() throws RemoteException, WorkloadException {

        for (final IWorker runningWorker : scriptedInstances.values()) {
            runningWorker.resumeWorkloads();
        }

    }

    public IWorker startH2OInstance(final boolean noReplicate) throws RemoteException, StartupException {

        return coord.startH2OInstance(noReplicate);
    }

    public H2OPropertiesWrapper getDescriptorFile() {

        return coord.getDescriptorFile();
    }

    public void addToFailureLog(final FailureLogEntry failureLogEntry) {

        failureLog.add(failureLogEntry);
    }

    public void addKillOrder(final Integer id_int, final long l) {

        killMonitor.addKillOrder(id_int, l);
    }

    public long getCurrentExecutionTime() {

        return currentExecutionTime;
    }

    public Collection<IWorker> getScriptedInstanceValues() {

        return scriptedInstances.values();
    }

    public Coordinator getCoordintor() {

        return coord;
    }

    public void addToCurrentExecutionTime(final long sleepTime) {

        currentExecutionTime += sleepTime;
    }

    public boolean hasStartedExecution() {

        return startedExecution;
    }

    public void startExecution() {

        currentExecutionTime = System.currentTimeMillis();
        startedExecution = true;
    }

    public List<FailureLogEntry> getFailureLog() {

        return failureLog;
    }

    public void disableKillMonitor() {

        if (killMonitor != null) {
            killMonitor.setRunning(false);
        }
    }

    public void startKillMonitor() {

        killMonitor = new KillMonitorThread(coord);

        if (killMonitor.isRunning()) {
            killMonitor.setRunning(false);
            killMonitor = new KillMonitorThread(coord);
        }

        killMonitor.start();

    }

}
