package org.h2o.eval.script.coord.specification;

public class TableClustering {

    private final Clustering clustering;

    /**
     * If the clustering is set to {@link Clustering#GROUPED} then this specifies how many tables will
     * be co-located in each group.
     */
    private int groupSize;

    public static enum Clustering {
        COLOCATED, SPREAD, GROUPED
    };

    private TableClustering(final Clustering clustering) {

        this.clustering = clustering;

    }

    public TableClustering(final Clustering clustering, final int groupSize) {

        this(clustering);
        this.groupSize = groupSize;
    }

    public int getGroupSize() {

        return groupSize;
    }

    public Clustering getClustering() {

        return clustering;
    }
}
