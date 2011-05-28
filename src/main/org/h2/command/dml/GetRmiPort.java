package org.h2.command.dml;

import java.sql.SQLException;
import java.sql.Statement;

import org.h2.command.ddl.SchemaCommand;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2.table.TableLinkConnection;
import org.h2o.db.manager.PersistentSystemTable;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class GetRmiPort extends SchemaCommand {

    private final String databaseLocation;

    private TableLinkConnection conn;

    /**
     * @param session
     * @param internalQuery
     * @param databaseLocation
     */
    public GetRmiPort(final Session session, final Schema schema, final String databaseLocation) {

        super(session, schema);

        this.databaseLocation = databaseLocation;
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

        if (databaseLocation == null) {
            /*
             * Return the RMI port on which this database is running.
             */

            return super.session.getDatabase().getID().getRMIPort();
        }
        else {
            /*
             * Send this query remotely to get the RMI port.
             */
            return pushCommand(databaseLocation, "GET RMI PORT", false);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#update(java.lang.String)
     */
    @Override
    public int update(final String transactionName) throws SQLException, RPCException {

        return update();
    }

    /**
     * Push a command to a remote machine where it will be properly executed.
     * 
     * @param createReplica
     *            true, if the command being pushed is a create replica command. This results in any subsequent tables involved in the
     *            command also being pushed.
     * @return The result of the update.
     * @throws SQLException
     * @throws RPCException
     */
    private int pushCommand(final String remoteDBLocation, final String query, final boolean clearLinkConnectionCache) throws SQLException, RPCException {

        final Database db = session.getDatabase();

        conn = db.getLinkConnection("org.h2.Driver", remoteDBLocation, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD, clearLinkConnectionCache);

        int result = -1;

        synchronized (conn) {
            try {
                final Statement stat = conn.getConnection().createStatement();

                stat.execute(query);
                result = stat.getUpdateCount();

            }
            catch (final SQLException e) {

                if (!clearLinkConnectionCache) {
                    pushCommand(remoteDBLocation, query, true);
                }
                else {

                    conn.close();
                    conn = null;
                    e.printStackTrace();
                    throw e;
                }
            }
        }

        return result;
    }
}
