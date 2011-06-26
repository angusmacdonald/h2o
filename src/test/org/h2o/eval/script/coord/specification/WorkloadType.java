package org.h2o.eval.script.coord.specification;

public class WorkloadType {

    public static enum LinkToTableLocation {
        WORKLOAD_PER_TABLE, //creates a single workload for every table in the database. 
        ALLENCOMPASSING_WORKLOAD, //creates a single workload for the entire database, querying each table in the database.
        GROUPED_WORKLOAD //creates a number of workloads which each query a distinct group of tables.
    };

    /**
     * The ratio of reads to writes in the benchmark. 0 = all writes, 1 = all reads.
     */
    private final double readWriteRatio;

    /**
     * If true queries will be directed against the System Table (CREATE, DROP). If false, they will be
     * directed against table managers (INSERT, UPDATE, DELETE).
     */
    private final boolean queryAgainstSystemTable;

    /**
     * How long to sleep between queries/updates to the database.
     */
    private final int sleepTime;

    /**
     * Whether queries/updates should take place within larger transactions (true) or
     * they should all be executed individually.
     */
    private final boolean multiQueryTransactionsEnabled;

    /**
     * If {@link #multiQueryTransactionsEnabled} is true, how many queries should
     * make up each transaction.
     */
    private final int queriesPerTransaction;

    /**
     * Specifies how workloads will be located in relation to tables.
     */
    private final LinkToTableLocation linkToTableLocation;

    /**
     * If true queries will be made on the same instance as the Table Manager/System Table is located. This setting relies
     * on the type of workload matching up to the {@link TableClustering}, which may make it impossible to co-locate for all tables.
     */
    private final boolean queriesLocalToTables;

    /**
    * @param readWriteRatio The ratio of reads to writes in the benchmark. 0 = all writes, 1 = all reads.
    * @param queryAgainstSystemTable If true queries will be directed against the System Table (CREATE, DROP). If false, they will be directed against table managers (INSERT, UPDATE, DELETE).
    * @param sleepTime  How long to sleep between queries/updates to the database.
    * @param multiQueryTransactionsEnabled Whether queries/updates should take place within larger transactions (true) or they should all be executed individually.
    * @param queriesPerTransaction If {@link #multiQueryTransactionsEnabled} is true, how many queries should make up each transaction.
    * @param linkToTableLocation Specifies how workloads will be located in relation to tables.
    * @param queriesLocalToTables If true queries will be made on the same instance as the Table Manager/System Table is located. This setting relies on the type of workload matching up to the {@link TableClustering}, which may make it impossible to co-locate for all tables.
    */
    public WorkloadType(final double readWriteRatio, final boolean queryAgainstSystemTable, final int sleepTime, final boolean multiQueryTransactionsEnabled, final int queriesPerTransaction, final LinkToTableLocation linkToTableLocation, final boolean queriesLocalToTables) {

        this.readWriteRatio = readWriteRatio;
        this.queryAgainstSystemTable = queryAgainstSystemTable;
        this.sleepTime = sleepTime;
        this.multiQueryTransactionsEnabled = multiQueryTransactionsEnabled;
        this.queriesPerTransaction = queriesPerTransaction;
        this.linkToTableLocation = linkToTableLocation;
        this.queriesLocalToTables = queriesLocalToTables;
    }

    public double getReadWriteRatio() {

        return readWriteRatio;
    }

    public boolean isQueryAgainstSystemTable() {

        return queryAgainstSystemTable;
    }

    public int getSleepTime() {

        return sleepTime;
    }

    public boolean isMultiQueryTransactionsEnabled() {

        return multiQueryTransactionsEnabled;
    }

    public int getQueriesPerTransaction() {

        return queriesPerTransaction;
    }

    public LinkToTableLocation getLinkToTableLocation() {

        return linkToTableLocation;
    }

    public boolean isQueriesLocalToTables() {

        return queriesLocalToTables;
    }

}
