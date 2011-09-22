package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.eval.script.workload.FailureLogEntry;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class MachineInstruction implements Instruction {

    private static final long serialVersionUID = 3278185465251127392L;

    /**
     * ID to be given to this machine being started.
     */
    public final Integer id;

    /**
     * Optional field. How long in milliseconds before the machine is terminated.
     */
    public final Long fail_after;

    public final boolean blockWorkloads;

    /**
     * True if the machine is being started, false if it is being terminated.
     */
    public boolean startMachine;

    public MachineInstruction(final Integer id, final Long fail_after, final boolean blockWorkloads, final boolean startMachine) {

        this.id = id;
        this.fail_after = fail_after;
        this.blockWorkloads = blockWorkloads;
        this.startMachine = startMachine;

    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException {

        if (startMachine) {
            startMachine(coordState);
        }
        else {
            terminateMachine(coordState);
        }
    }

    private void terminateMachine(final CoordinatorScriptState coordState) throws RemoteException {

        try {
            coordState.getCoordintor().killInstance(id);
        }
        catch (final ShutdownException e) {
            ErrorHandling.exceptionError(e, "Failed to shutdown instance with ID " + id + ".");
        }
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "CSCRIPT: Terminated machine with ID '" + id + "'");

        coordState.addToFailureLog(new FailureLogEntry(coordState.getCurrentExecutionTime(), coordState.getScriptedInstance(id).getLocalDatabaseName(), false));

    }

    private void startMachine(final CoordinatorScriptState coordState) throws RemoteException, WorkloadException, StartupException {

        IWorker worker = coordState.getScriptedInstance(id);

        if (blockWorkloads) {
            coordState.blockWorkloads();
        }

        /*
         * Start machine. This blocks the co-ordinator script until the machine is restarted.
         */
        if (worker == null) { //Machine is being started for the first time.
            worker = coordState.startH2OInstance(id == 0, coordState.getScriptName(), coordState.getDiagnosticLevel()); //disable replication on the first instance.
            coordState.addScriptedInstance(id, worker);
        }
        else {
            //Machine is being restarted
            worker.startH2OInstance(coordState.getDescriptorFile(), false, id == 0, coordState.getScriptName(), coordState.getDiagnosticLevel());
            coordState.addScriptedInstance(id, worker);
            coordState.addToFailureLog(new FailureLogEntry(coordState.getCurrentExecutionTime(), coordState.getScriptedInstance(id).getLocalDatabaseName(), true));
        }

        if (blockWorkloads) {
            coordState.resumeWorkloads();
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "CSCRIPT: Starting machine with ID '" + id + "'");

        if (fail_after != null) {
            coordState.addKillOrder(id, System.currentTimeMillis() + fail_after);
        }
    }

}
