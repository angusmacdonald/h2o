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

public class QueryLogEntry extends LogEntry implements Serializable {

    private static final long serialVersionUID = 6421309505616188993L;

    /**
     * Type of query being performed.
     */
    public List<QueryType> queryTypes;

    /**
     * Name of the table on which this operation was performed.
     */
    public final Collection<String> tablesInvolved;

    /**
     * Time taken to execute the query (either successfully, or for it to fail).
     */
    public long timeToExecute;

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

        super(timeOfExecution);

        this.successfulExecution = successfulExecution;
        this.timeToExecute = timeToExecute;
        this.queryTypes = queryTypes;
        this.tablesInvolved = tablesInvolved;
    }

    public String toCSV(final DateFormat dateformatter, final String locationOfExecution, final Collection<String> tableNames, final long startTime) {

        return super.toCSV(dateformatter, locationOfExecution, tableNames, startTime, tablesInvolved, successfulExecution, timeToExecute, queryTypes, false);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "QueryLogEntry [queryType=" + PrettyPrinter.toString(queryTypes) + ", tableInvolved=" + PrettyPrinter.toString(tablesInvolved) + ", timeToExecute=" + timeToExecute + ", timeOfLogEntry=" + timeOfExecution + ", successfulExecution=" + successfulExecution + "]";
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

}
