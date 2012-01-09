package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2o.db.manager.recovery.SystemTableAccessException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrateSystemTable extends org.h2.command.ddl.SchemaCommand {

    private final boolean noReplicateToPreviousInstance;

    /**
     * @param session
     * @param schema
     * @param noReplicateToPreviousInstance If true, then the new system table will start and be told not to replicate data to the previous system table's location. Only applies to migrations on active System Tables (those not using persisted state).
     */
    public MigrateSystemTable(final Session session, final Schema schema, final boolean noReplicateToPreviousInstance) {

        super(session, schema);
        this.noReplicateToPreviousInstance = noReplicateToPreviousInstance;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#isTransactional()
     */
    @Override
    public boolean isTransactional() {

        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#update()
     */
    @Override
    public int update() throws SQLException, RPCException {

        try {
            session.getDatabase().getSystemTableReference().migrateSystemTableToLocalInstance(noReplicateToPreviousInstance);
        }
        catch (final SystemTableAccessException e) {
            System.err.println("----");
            e.printStackTrace();
            final SQLException sqlException = new SQLException(e);
            sqlException.setStackTrace(e.getStackTrace());

            throw sqlException;
        }

        return 0;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#update(java.lang.String)
     */
    @Override
    public int update(final String transactionName) throws SQLException, RPCException {

        return update();
    }

}
