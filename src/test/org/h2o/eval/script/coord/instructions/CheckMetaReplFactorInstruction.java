package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class CheckMetaReplFactorInstruction implements Instruction {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final String tableName;
    private final int expected;

    public CheckMetaReplFactorInstruction(final String tableName, final int expected) {

        this.tableName = tableName;
        this.expected = expected;

        // TODO Auto-generated constructor stub
    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException, SQLException {

        final String query = "GET META-REPLICATION FACTOR " + tableName;

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing query '" + query + "' on '0'.");

        final IWorker worker = coordState.getScriptedInstance(0); //always perform query on first machine.

        final int replication_factor = coordState.getCoordintor().executeQuery(worker, query);

        if (replication_factor != expected) { throw new WorkloadException("Replication factor was not correct. Expected " + expected + ", but it was " + replication_factor); }

    }

}
