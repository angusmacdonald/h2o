package org.h2o.eval.script.workload;

import java.io.Serializable;

public class QueryLogEntry implements Serializable {

    private static final long serialVersionUID = 6421309505616188993L;

    public enum QueryType {
        CREATE, DROP, INSERT, DELETE, UNKNOWN
    };

    /**
     * Type of query being performed.
     */
    public QueryType queryType;

    /**
     * Name of the table on which this operation was performed.
     */
    public String tableInvolved;

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
     * When the commit occurred.
     */
    public final long timeOfExecution;

    /**
     * @param successfulExecution
     * @param timeToExecute
     * @param queryType
     * @param tableInvolved
     * @param timeOfExecution 
     */
    public QueryLogEntry(final boolean successfulExecution, final long timeToExecute, final QueryType queryType, final String tableInvolved, final long timeOfExecution) {

        this.timeOfExecution = timeOfExecution;
        timeOfLogEntry = System.currentTimeMillis();
        this.successfulExecution = successfulExecution;
        this.timeToExecute = timeToExecute;
        this.queryType = queryType;
        this.tableInvolved = tableInvolved;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "QueryLogEntry [queryType=" + queryType + ", tableInvolved=" + tableInvolved + ", timeToExecute=" + timeToExecute + ", timeOfLogEntry=" + timeOfLogEntry + ", successfulExecution=" + successfulExecution + ", timeOfExecution=" + timeOfExecution + "]";
    }

    public static QueryLogEntry createQueryLogEntry(final String query, final boolean successfullyExecuted, final long timeToExecute) {

        QueryType queryType = QueryType.UNKNOWN;
        String tableInvolved = "Unknown";

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

        return new QueryLogEntry(successfullyExecuted, timeToExecute, queryType, tableInvolved, System.currentTimeMillis());

    }

}
