package org.h2o.eval.script.coord.specification;

public class WorkloadType {

    public static enum LinkToTableLocation {
        WORKLOAD_PER_TABLE, //creates a single workload for every table in the database. 
        ALL_ENCOMPASSING_WORKLOAD, //creates a single workload for the entire database, querying each table in the database.
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
    * @param queryAgainstSystemTable If true queries will be directed against the System Table (CREATE, DROP). If false, they will be directed against table managers (INSERT, UPDATE, DELETE). If directed against the system table the parameters for {@link #multiQueryTransactionsEnabled}
    * and {@link #readWriteRatio} are ignored.
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

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (linkToTableLocation == null ? 0 : linkToTableLocation.hashCode());
        result = prime * result + (multiQueryTransactionsEnabled ? 1231 : 1237);
        result = prime * result + (queriesLocalToTables ? 1231 : 1237);
        result = prime * result + queriesPerTransaction;
        result = prime * result + (queryAgainstSystemTable ? 1231 : 1237);
        long temp;
        temp = Double.doubleToLongBits(readWriteRatio);
        result = prime * result + (int) (temp ^ temp >>> 32);
        result = prime * result + sleepTime;
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final WorkloadType other = (WorkloadType) obj;
        if (linkToTableLocation != other.linkToTableLocation) { return false; }
        if (multiQueryTransactionsEnabled != other.multiQueryTransactionsEnabled) { return false; }
        if (queriesLocalToTables != other.queriesLocalToTables) { return false; }
        if (queriesPerTransaction != other.queriesPerTransaction) { return false; }
        if (queryAgainstSystemTable != other.queryAgainstSystemTable) { return false; }
        if (Double.doubleToLongBits(readWriteRatio) != Double.doubleToLongBits(other.readWriteRatio)) { return false; }
        if (sleepTime != other.sleepTime) { return false; }
        return true;
    }

}
