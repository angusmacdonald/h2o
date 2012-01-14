package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class CheckReplFactorInstruction implements Instruction {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final String tableName;
    private final int expected;

    public CheckReplFactorInstruction(final String tableName, final int expected) {

        this.tableName = tableName;
        this.expected = expected;

        // TODO Auto-generated constructor stub
    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException, SQLException {

        final String query = "GET REPLICATION FACTOR " + tableName;

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing query '" + query + "' on '0'.");

        final IWorker worker = coordState.getScriptedInstance(0); //always perform query on first machine.

        int replication_factor = 0;
        try {
            replication_factor = coordState.getCoordintor().executeQuery(worker, query);
        }
        catch (final SQLException e) {
            // This can be a valid exception if the system can't connect to a System Table (which is a use case in some scripts);
            ErrorHandling.errorNoEvent("Couldn't execute meta-data replication factor query: " + e.getMessage());
        }

        if (replication_factor != expected) { throw new WorkloadException("Replication factor was not correct. Expected " + expected + ", but it was " + replication_factor); }

    }

}
