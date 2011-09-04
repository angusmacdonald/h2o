package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;

import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class SleepInstruction implements Instruction {

    private static final long serialVersionUID = -721457607878391357L;

    private final Integer sleepTime;

    public SleepInstruction(final Integer sleepTime) {

        this.sleepTime = sleepTime;

    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "CSCRIPT: Sleeping for '" + sleepTime + "'");

        if (coordState.hasStartedExecution()) {
            coordState.addToCurrentExecutionTime(sleepTime);
        }

        try {
            Thread.sleep(sleepTime);
        }
        catch (final InterruptedException e) {
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "CSCRIPT: Finished sleeping.");

    }

}
