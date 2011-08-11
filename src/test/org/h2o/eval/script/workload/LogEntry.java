package org.h2o.eval.script.workload;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Collection;
import java.util.List;

import uk.ac.standrews.cs.nds.util.PrettyPrinter;

public class LogEntry implements Serializable {

    private static final long serialVersionUID = 8404704261377608930L;

    public long timeOfExecution;

    public LogEntry() {

    }

    public LogEntry(final long timeOfExecution) {

        this.timeOfExecution = timeOfExecution;
    }

    public static String toCSVHeader(final Collection<String> tableNames) {

        String header = "Time of Transaction (ms), Time of Transaction (s), Time To Execute if Successful, Time to Execute if Unsuccessful, Location of Event, Tables Involved, Insert Queries, Select Queries, ";

        for (final String tableName : tableNames) {
            header += tableName + ",";
        }

        header += "\n";

        return header;
    }

    public String toCSV(final DateFormat dateformatter, final String locationOfExecution, final Collection<String> tableNames, final long startTime, final Collection<String> tablesInvolved, final boolean successfulExecution, final long timeToExecute, final List<QueryType> queryTypes) {

        final String tablesInvolvedString = PrettyPrinter.toString(tablesInvolved, ";", false);
        final long timeOfTransactionMS = timeOfExecution - startTime;
        final int timeOfTransactionSec = (int) (timeOfTransactionMS / 1000);

        final String successfulExecutionTime = (successfulExecution ? timeToExecute : "=NA()") + "";
        final String unsuccessfulExecutionTime = (!successfulExecution ? timeToExecute : "=NA()") + "";

        String row = timeOfTransactionMS + "," + timeOfTransactionSec + "," + successfulExecutionTime + "," + unsuccessfulExecutionTime + ", " + locationOfExecution + "," + tablesInvolvedString + "," + getNumberOf(QueryType.INSERT, queryTypes) + "," + getNumberOf(QueryType.SELECT, queryTypes) + ",";

        if (tableNames != null) {
            for (final String tableName : tableNames) {
                if (tableName.equals(tablesInvolvedString)) {
                    row += timeToExecute;
                }
                else {
                    row += "=NA()";
                }

                row += ",";
            }
        }
        row += "\n";

        return row;

    }

    public enum QueryType {
        CREATE, DROP, INSERT, DELETE, UNKNOWN, SELECT
    };

    int getNumberOf(final QueryType type, final List<QueryType> queryTypes) {

        if (queryTypes == null) { return 0; }
        int count = 0;
        for (final QueryType singleQueryType : queryTypes) {
            if (singleQueryType.equals(type)) {
                count++;
            }
        }
        return count;
    }

}
