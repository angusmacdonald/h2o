package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2o.db.query.TableProxyManager;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class SetReplicate extends Prepared {

    private final boolean allowReplication;

    public SetReplicate(final Session session, final boolean allowReplication) {

        super(session, true);
        this.allowReplication = allowReplication;
    }

    @Override
    public int update() throws SQLException, RPCException {

        session.getDatabase().setReplicate(allowReplication);

        return 1;
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
