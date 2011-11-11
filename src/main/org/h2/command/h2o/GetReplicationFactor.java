package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2o.db.id.TableInfo;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * Returns the replication factor of a given table.
 * 
 * Syntax: GET REPLICATION FACTOR <table_name>
 * @author angus
 *
 */
public class GetReplicationFactor extends Prepared {

    private final String tableName;

    public GetReplicationFactor(final Session session, final String tableName) {

        super(session, true);
        this.tableName = tableName;
    }

    @Override
    public int update() throws SQLException, RPCException {

        try {
            final TableManagerWrapper tm = session.getDatabase().getSystemTable().lookup(new TableInfo("PUBLIC." + tableName));

            final int currentReplicationFactor = tm.getTableManager().getReplicasOnActiveMachines().size();

            return currentReplicationFactor;
        }
        catch (final MovedException e) {
            e.printStackTrace();
        }

        return -1; //indicating failure
    }

    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        //No lock required.
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#isTransactionCommand()
     */
    @Override
    public boolean isTransactionCommand() {

        return true;
    }

    @Override
    public boolean isTransactional() {

        return false;
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        // TODO Auto-generated method stub
        return null;
    }

}
