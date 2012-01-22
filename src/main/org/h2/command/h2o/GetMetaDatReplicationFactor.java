package org.h2.command.h2o;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.db.query.TableProxyManager;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.PrettyPrinter;

/**
 * Returns the replication factor of a given table.
 * 
 * Syntax: GET META-REPLICATION FACTOR <table_name>  -- table name optional.
 * @author angus
 *
 */
public class GetMetaDatReplicationFactor extends Prepared {

    private final String tableName;

    public GetMetaDatReplicationFactor(final Session session, final String tableName) {

        super(session, true);
        this.tableName = tableName;
    }

    @Override
    public int update() throws SQLException, RPCException {

        try {
            if (tableName == null || tableName.equals("NULL")) {
                if (session.getDatabase().isConnected()) {

                    return session.getDatabase().getSystemTable().getCurrentSystemTableReplication();

                }
                else {
                    return 0;
                }
            }
            else {
                final TableInfo ti = new TableInfo("PUBLIC." + tableName);
                if (!session.getDatabase().isConnected()) {

                return 0;

                }
                final Map<TableInfo, Set<DatabaseID>> replicaLocations = session.getDatabase().getSystemTable().getReplicaLocations();
                System.err.println("-----");
                System.err.println(tableName);
                System.err.println(PrettyPrinter.toString(replicaLocations));
                final Set<DatabaseID> replicaLocationsForTi = replicaLocations.get(ti);

                Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Replication locations for " + ti + ": " + PrettyPrinter.toString(replicaLocationsForTi));

                int currentReplicationFactor = 0;

                if (replicaLocationsForTi != null) {

                    for (final DatabaseID id : replicaLocationsForTi) {

                        try {

                            final IDatabaseInstanceRemote dir = session.getDatabase().getRemoteInterface().getDatabaseInstanceAt(id);

                            if (dir.isAlive()) {
                                currentReplicationFactor++;
                            }
                        }
                        catch (final Exception e) {
                            e.printStackTrace();
                            //Machine has failed.
                        }
                    }

                }

                return currentReplicationFactor;
            }
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
