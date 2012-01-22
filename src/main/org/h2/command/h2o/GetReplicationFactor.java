package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2o.db.id.TableInfo;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.db.wrappers.TableManagerWrapper;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.PrettyPrinter;

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

            int replFactor = 0;

            for (final DatabaseInstanceWrapper wrapper : tm.getTableManager().getReplicasOnActiveMachines().keySet()) {
                try {

                    wrapper.getDatabaseInstance().getChordPort();
                    if (wrapper.getDatabaseInstance().isAlive()) {
                        replFactor++;
                    }
                }
                catch (final Exception e) {
                    e.printStackTrace();
                    //Machine has failed.
                }
            }

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Replication factor for " + tableName + " is " + replFactor + " (" + PrettyPrinter.toString(tm.getTableManager().getReplicasOnActiveMachines()) + ")");

            return replFactor;
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
