package org.h2o.eval.script.coord.instructions;

import java.io.File;
import java.rmi.RemoteException;

import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.FileUtil;

public class WorkloadInstruction implements Instruction {

    private static final long serialVersionUID = -5175906689517121113L;

    /**
     * Workload to be executed.
     */
    public final String workloadFilePath;

    /**
     * How long the workload should be executed for.
     */
    public final Long duration;

    private final String id;

    public WorkloadInstruction(final String id, final String workloadFilePath, final Long duration) {

        this.id = id;
        this.duration = duration == null ? 0 : duration;
        System.out.println("workloadFilePath: " + workloadFilePath);
        this.workloadFilePath = workloadFilePath;

    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException {

        String workloadFileLocationToUse = workloadFilePath;

        //test whether file exists. if it doesn't, try a path relative to the co-ordinator script. If that doesn't work, fail.

        if (!FileUtil.exists(workloadFileLocationToUse)) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Workload file not found at: " + workloadFileLocationToUse);

            //try a relative path from the co-ordinator path.
            workloadFileLocationToUse = getCoordinatorFilePath(coordState.getScriptFileLocation()) + File.separator + workloadFilePath;

            if (!FileUtil.exists(workloadFileLocationToUse)) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Workload file not found at: " + workloadFileLocationToUse);

                throw new WorkloadException("The workload file (at '" + workloadFilePath + "') could not be found.");
            }
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Workload file found at: " + workloadFileLocationToUse);

        coordState.addNewWorkloadLength(duration);
        coordState.getCoordintor().executeWorkload(id, workloadFileLocationToUse, duration);

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing workload '" + workloadFilePath + "' for '" + duration + "', on '" + id + "'.");

        if (!coordState.hasStartedExecution()) {
            coordState.startExecution();

        }
    }

    private String getCoordinatorFilePath(final String fullScriptLocation) {

        if (fullScriptLocation.lastIndexOf("/") > 0) {
            return fullScriptLocation.substring(0, fullScriptLocation.lastIndexOf("/"));
        }
        else if (fullScriptLocation.lastIndexOf("\\") > 0) {
            return fullScriptLocation.substring(0, fullScriptLocation.lastIndexOf("\\"));
        }
        else {
            return fullScriptLocation;
        }
    }

}
