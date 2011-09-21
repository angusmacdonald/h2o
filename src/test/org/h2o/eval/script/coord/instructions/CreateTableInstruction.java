package org.h2o.eval.script.coord.instructions;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.WorkloadException;
import org.h2o.eval.script.workload.WorkloadExecutor;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class CreateTableInstruction implements Instruction {

    private static final long serialVersionUID = -1808573817645414542L;

    private final String machineToCreateTableOn;
    private final String tableName;
    private final String tableSchema; //e.g.  "id int, str_a varchar(40), int_a BIGINT"
    private final int prepopulateWith;

    public CreateTableInstruction(final String machineToCreateTableOn, final String tableName, final String tableSchema, final String prepopulateWith) {

        this.machineToCreateTableOn = machineToCreateTableOn;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.prepopulateWith = prepopulateWith != null ? Integer.valueOf(prepopulateWith) : 0;

    }

    @Override
    public void execute(final CoordinatorScriptState coordState) throws RemoteException, StartupException, WorkloadException, SQLException {

        final String createTableQuery = "CREATE TABLE " + tableName + " (" + tableSchema + ");";

        final IWorker worker = coordState.getScriptedInstance(Integer.valueOf(machineToCreateTableOn));

        coordState.getCoordintor().executeQuery(worker, createTableQuery);

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Executing query '" + createTableQuery + "' on '" + machineToCreateTableOn + "'.");

        if (prepopulateWith > 0) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "CSCRIPT: Prepopulating table '" + tableName + "' with '" + prepopulateWith + "' rows.");
            //TODO pre-populate.

            String query = "INSERT INTO " + tableName + " VALUES (";

            final String[] attributes = tableSchema.split(",");

            //XXX this is very fragile with regards to the exact format/case of the SQL.
            for (int i = 0; i < attributes.length; i++) {
                query += i > 0 ? ", " : "";

                if (attributes[i].contains(" varchar")) {
                    query += WorkloadExecutor.GENERATED_STRING_PLACEHOLDER;
                }
                else if (attributes[i].contains(" BIGINT")) {
                    query += WorkloadExecutor.GENERATED_LONG_PLACEHOLDER;
                }
                else if (attributes[i].contains(" int")) {
                    query += WorkloadExecutor.LOOP_COUNTER_PLACEHOLDER;
                }
            }
            query += ");";

            final ArrayList<String> workload = createWorkloadAroundQuery(query);

            coordState.getCoordintor().executeWorkload(machineToCreateTableOn, workload, 0, true);

        }
    }

    private ArrayList<String> createWorkloadAroundQuery(final String query) {

        final ArrayList<String> workload = new ArrayList<String>();

        workload.add("<loop iterations=\"" + prepopulateWith + "\"/>");

        workload.add(query);

        workload.add("<increment/>");
        workload.add(WorkloadExecutor.LOOP_END_TAG);
        return workload;
    }
}
