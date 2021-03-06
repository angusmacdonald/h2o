package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.TableManager;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class RecreateTableManager extends org.h2.command.ddl.SchemaCommand {

    private final String tableName;

    private final String oldPrimaryLocation;

    /**
     * @param session
     * @param schema
     */
    public RecreateTableManager(final Session session, final Schema schema, final String tableName, String oldPrimaryLocation) {

        super(session, schema);

        if (oldPrimaryLocation.startsWith("'")) {
            oldPrimaryLocation = oldPrimaryLocation.substring(1);
        }
        if (oldPrimaryLocation.endsWith("'")) {
            oldPrimaryLocation = oldPrimaryLocation.substring(0, oldPrimaryLocation.length() - 1);
        }

        this.oldPrimaryLocation = oldPrimaryLocation;

        this.tableName = tableName;
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

        final Database db = session.getDatabase();
        final ISystemTableReference systemTableReference = db.getSystemTableReference();

        String schemaName = "";
        if (getSchema() != null) {
            schemaName = getSchema().getName();
        }
        else {
            schemaName = "PUBLIC";
        }

        final TableInfo ti = new TableInfo(tableName, schemaName, db.getID());

        /*
         * Perform a check to see that it isn't already active.
         */
        final ITableManagerRemote extantTableManager = systemTableReference.lookup(ti, false);

        boolean tableManagerExists = false;

        try {
            tableManagerExists = extantTableManager.isAlive();
        }
        catch (final Exception e1) {
            //Expected.
        }

        if (!tableManagerExists) {
            //Create the new table manager on this instance.
            TableManager tm = null;

            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, db.getID() + ": Recreating table manager for " + ti + ", with state from " + oldPrimaryLocation + "'s replicated meta-tables.");

            try {
                tm = new TableManager(ti, db, true);
                tm.recreateReplicaManagerState(oldPrimaryLocation);
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Re-created replica manager for " + ti + " on " + db.getID() + ".");
                tm.persistToCompleteStartup(ti);
                tm.persistReplicaInformation();
                H2OEventBus.publish(new H2OEvent(db.getID().getURL(), DatabaseStates.TABLE_MANAGER_CREATION, ti.getFullTableName()));
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Persisted to complete startup, recreation of table mnanager for " + ti + " on " + db.getID() + ".");

            }
            catch (final SQLException e) {
                //Update Failed.
                return -1;
            }
            catch (final Exception e) {
                //Update Failed.
                return -1;
            }

            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, ti + " recreated on " + db.getID() + ".");

            systemTableReference.addNewTableManagerReference(ti, tm);
        }
        else {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Table Manager for " + ti + " will not be recreated on " + db.getID() + " because it already active elsewhere.");
        }

        return 1;
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
