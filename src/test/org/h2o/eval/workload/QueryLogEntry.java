package org.h2o.eval.workload;

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
     * @param successfulExecution
     * @param timeToExecute
     * @param queryType
     * @param tableInvolved
     */
    public QueryLogEntry(final boolean successfulExecution, final long timeToExecute, final QueryType queryType, final String tableInvolved) {

        timeOfLogEntry = System.currentTimeMillis();
        this.successfulExecution = successfulExecution;
        this.timeToExecute = timeToExecute;
        this.queryType = queryType;
        this.tableInvolved = tableInvolved;
    }

    @Override
    public String toString() {

        return "QueryLogEntry [queryType=" + queryType + ", tableInvolved=" + tableInvolved + ", timeToExecute=" + timeToExecute + ", successfulExecution=" + successfulExecution + "]";
    }

}
