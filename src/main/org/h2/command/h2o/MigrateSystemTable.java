package org.h2.command.h2o;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2o.db.manager.recovery.SystemTableAccessException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrateSystemTable extends org.h2.command.ddl.SchemaCommand {

    /**
     * @param session
     * @param schema
     */
    public MigrateSystemTable(Session session, Schema schema) {

        super(session, schema);
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
    public int update() throws SQLException, RemoteException {

        try {
            this.session.getDatabase().getSystemTableReference().migrateSystemTableToLocalInstance();
        }
        catch (SystemTableAccessException e) {
            throw new SQLException("Failed to recreate System Table on this machine.");
        }

        return 0;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#update(java.lang.String)
     */
    @Override
    public int update(String transactionName) throws SQLException, RemoteException {

        return update();
    }

}
