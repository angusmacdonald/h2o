package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class QueryInstruction implements Instruction {

    private static final long serialVersionUID = -2808571499092300184L;

    /**
     * Query to be executed.
     */
    public final String query;

    /**
     * Machine which should execute a given instruction.
     */
    public final String id;

    public QueryInstruction(final String id, final String query) {

        this.id = id;
        this.query = query;
    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException, SQLException {

        final IWorker worker = coordState.getScriptedInstance(Integer.valueOf(id));

        coordState.getCoordintor().executeQuery(worker, query);

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing query '" + query + "' on '" + id + "'.");

    }

}
