package org.h2o.autonomic.numonic.ranking;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.interfaces.ISystemTableReference;

import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

/**
 * This class collects incoming monitoring data, then sends it to the System Table when enough data has been collected.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class ResourceRanker {

    /**
     * How this set of data will be identified when it is sent to the System Table.
     */
    private final DatabaseID localDatabaseID;

    /**
     * Where data will be sent.
     */
    private final ISystemTableReference systemTable;

    /**
     * @param localDatabaseID How this set of data will be identified when it is sent to the System Table.
     * @param systemTable Where data will be sent.
     */
    public ResourceRanker(final DatabaseID localDatabaseID, final ISystemTableReference systemTable) {

        this.localDatabaseID = localDatabaseID;
        this.systemTable = systemTable;
    }

    public void collateRankingData(final SingleSummary<? extends Data> specificFsSummary) {

        // TODO Auto-generated method stub

    }

}
