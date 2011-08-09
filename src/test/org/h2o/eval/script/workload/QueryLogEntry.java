package org.h2o.eval.script.workload;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.PrettyPrinter;

public class QueryLogEntry implements Serializable {

    private static final long serialVersionUID = 6421309505616188993L;

    public enum QueryType {
        CREATE, DROP, INSERT, DELETE, UNKNOWN, SELECT
    };

    /**
     * Type of query being performed.
     */
    public List<QueryType> queryTypes;

    /**
     * Name of the table on which this operation was performed.
     */
    public final Set<String> tablesInvolved;

    /**
     * Time taken to execute the query (either successfully, or for it to fail).
     */
    public long timeToExecute;

    public long timeOfLogEntry;

    /**
     * Whether the query executed successfully.
     */
    public boolean successfulExecution;

    /**
     * @param successfulExecution
     * @param timeToExecute
     * @param queryTypes
     * @param tablesInvolved
     * @param timeOfExecution 
     */
    public QueryLogEntry(final long timeOfExecution, final boolean successfulExecution, final long timeToExecute, final List<QueryType> queryTypes, final Set<String> tablesInvolved) {

        timeOfLogEntry = timeOfExecution;
        this.successfulExecution = successfulExecution;
        this.timeToExecute = timeToExecute;
        this.queryTypes = queryTypes;
        this.tablesInvolved = tablesInvolved;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "QueryLogEntry [queryType=" + PrettyPrinter.toString(queryTypes) + ", tableInvolved=" + PrettyPrinter.toString(tablesInvolved) + ", timeToExecute=" + timeToExecute + ", timeOfLogEntry=" + timeOfLogEntry + ", successfulExecution=" + successfulExecution + "]";
    }

    public static QueryLogEntry createQueryLogEntry(final long timeOfExecution, final String query, final boolean successfullyExecuted, final long timeToExecute) {

        final List<String> queries = new LinkedList<String>();
        queries.add(query);

        return createQueryLogEntry(timeOfExecution, queries, successfullyExecuted, timeToExecute);
    }

    public static QueryLogEntry createQueryLogEntry(final long timeOfExecution, final List<String> queriesInThisTransaction, final boolean successfullyExecuted, final long timeToExecute) {

        final Set<String> tablesInvolved = new HashSet<String>();
        final List<QueryType> queryTypes = new LinkedList<QueryLogEntry.QueryType>();
        for (final String query : queriesInThisTransaction) {

            QueryType queryType = QueryType.UNKNOWN;
            String tableInvolved = null;

            if (query.contains("INSERT INTO")) {
                queryType = QueryType.INSERT;

                tableInvolved = query.substring("INSERT INTO ".length(), query.indexOf(" VALUES ("));
            }
            else if (query.contains("DELETE FROM")) {
                queryType = QueryType.DELETE;
                tableInvolved = query.substring("DELETE FROM ".length(), query.indexOf(" WHERE"));
            }
            else if (query.contains("CREATE TABLE")) {
                queryType = QueryType.CREATE;
                tableInvolved = query.substring("CREATE TABLE ".length(), query.indexOf(" ("));
            }
            else if (query.contains("DROP TABLE")) {
                queryType = QueryType.DROP;
                tableInvolved = query.substring("DROP TABLE ".length(), query.indexOf(";"));
            }

            else if (query.contains("SELECT * FROM ")) {
                queryType = QueryType.SELECT;
                tableInvolved = query.substring("SELECT * FROM ".length(), query.indexOf(" WHERE"));
            }

            if (tableInvolved != null) {
                tablesInvolved.add(tableInvolved);
            }
            queryTypes.add(queryType);
        }

        final QueryLogEntry newQueryLog = new QueryLogEntry(timeOfExecution, successfullyExecuted, timeToExecute, queryTypes, tablesInvolved);

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Created new query log entry: " + newQueryLog);
        return newQueryLog;

    }

    public String toCSV(final DateFormat dateformatter, final String locationOfExecution, final Collection<String> tableNames, final long startTime) {

        final String tablesInvolvedString = PrettyPrinter.toString(tablesInvolved, ";", false);
        final long timeOfTransactionMS = timeOfLogEntry - startTime;
        final int timeOfTransactionSec = (int) (timeOfTransactionMS / 1000);

        final String successfulExecutionTime = (successfulExecution ? timeToExecute : "=NA()") + "";
        final String unsuccessfulExecutionTime = (!successfulExecution ? timeToExecute : "=NA()") + "";

        String row = timeOfTransactionMS + "," + timeOfTransactionSec + "," + successfulExecutionTime + "," + unsuccessfulExecutionTime + ", " + locationOfExecution + "," + tablesInvolvedString + "," + getNumberOf(QueryType.INSERT) + "," + getNumberOf(QueryType.SELECT) + ",";

        for (final String tableName : tableNames) {
            if (tableName.equals(tablesInvolvedString)) {
                row += timeToExecute;
            }
            else {
                row += "=NA()";
            }

            row += ",";
        }

        row += "\n";

        return row;

    }

    private int getNumberOf(final QueryType type) {

        int count = 0;
        for (final QueryType singleQueryType : queryTypes) {
            if (singleQueryType.equals(type)) {
                count++;
            }
        }
        return count;
    }

    public static String toCSVHeader(final Collection<String> tableNames) {

        String header = "Time of Transaction (ms), Time of Transaction (s), Time To Execute if Successful, Time to Execute if Unsuccessful, Location of Execution, Tables Involved, Insert Queries, Select Queries, ";

        for (final String tableName : tableNames) {
            header += tableName + ",";
        }

        header += "\n";

        return header;
    }
}
