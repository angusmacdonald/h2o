package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.TableManager;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.util.exceptions.MigrationException;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.rpc.RPCException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MigrateTableManager extends org.h2.command.ddl.SchemaCommand {

    private final String tableName;

    /**
     * @param session
     * @param schema
     */
    public MigrateTableManager(final Session session, final Schema schema, final String tableName) {

        super(session, schema);

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

        int result = -1;

        try {
            final Database db = session.getDatabase();
            final ISystemTableReference sm = db.getSystemTableReference();
            String schemaName = "";
            if (getSchema() != null) {
                schemaName = getSchema().getName();
            }
            else {
                schemaName = "PUBLIC";
            }

            final TableInfo ti = new TableInfo(tableName, schemaName);
            ITableManagerRemote tableManager = sm.lookup(ti, true);

            if (tableManager == null) {
                Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, getSchema().getName() + tableName);
            }

            TableProxy qp = null;
            try {
                qp = tableManager.getTableProxy(LockType.WRITE, new LockRequest(session));
            }
            catch (final MovedException e) {
                tableManager = sm.lookup(new TableInfo(tableName, schemaName), false);
                qp = tableManager.getTableProxy(LockType.WRITE, new LockRequest(session));
            }

            if (!qp.getLockGranted().equals(LockType.NONE)) {
                result = migrateTableManagerToLocalInstance(tableManager, schemaName, db);
            }
            else {
                throw Message.getSQLException(ErrorCode.LOCK_TIMEOUT_1, getSchema().getName() + tableName);
            }

            H2OEventBus.publish(new H2OEvent(db.getURL().getURL(), DatabaseStates.TABLE_MANAGER_MIGRATION, ti.getFullTableName()));

        }
        catch (final MovedException e) {
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, getSchema().getName() + tableName);
        }
        catch (final SQLException e) {
            throw e;
        }
        catch (final Exception e) {
            throw new SQLException("Failed to migrate table manager for " + getSchema().getName() + "." + tableName + ".");
        }

        return result;
    }

    public int migrateTableManagerToLocalInstance(ITableManagerRemote oldTableManager, final String schemaName, final Database db) throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Preparing to migrate Table Manager for [" + schemaName + "." + tableName);

        /*
         * Create a new System Table instance locally.
         */
        ITableManagerRemote newTableManager = null;

        final TableInfo ti = new TableInfo(tableName, schemaName, 0l, 0, "TABLE", db.getURL());

        try {
            newTableManager = new TableManager(ti, db, true);
            newTableManager.persistToCompleteStartup(ti);
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Failed to create new Table Manager [" + schemaName + "." + tableName + "].");
        }

        /*
         * Stop the old, remote, manager from accepting any more requests.
         */
        try {
            oldTableManager.prepareForMigration(db.getURL().getURLwithRMIPort());
        }
        catch (final RPCException e) {
            e.printStackTrace();
        }
        catch (final MigrationException e) {
            throw new SQLException("This Table Manager [" + schemaName + "." + tableName + "] is already being migrated to another instance.");
        }
        catch (final MovedException e) {
            throw new SQLException("This Table Manager [" + schemaName + "." + tableName + "] is already being migrated to another instance.");
        }

        /*
         * Build the System Table's state from that of the existing manager.
         */
        try {
            newTableManager.buildTableManagerState(oldTableManager);
        }
        catch (final RPCException e) {
            e.printStackTrace();
            throw new SQLException("Failed to migrate Table Manager [" + schemaName + "." + tableName + "] to new machine.");
        }
        catch (final MovedException e) {
            throw new SQLException("This shouldn't be possible here. The Table Manager [" + schemaName + "." + tableName + "] has moved, but this instance should have had exclusive rights to it.");
        }

        /*
         * Shut down the old, remote, System Table. Redirect requests to new manager.
         */
        try {
            oldTableManager.completeMigration();
        }
        catch (final RPCException e) {
            throw new SQLException("Failed to complete migration [" + schemaName + "." + tableName + "].");

        }
        catch (final MovedException e) {
            throw new SQLException("This shouldn't be possible here. The Table Manager has moved, but this instance should have had exclusive rights to it.");

        }
        catch (final MigrationException e) {
            e.printStackTrace();
            throw new SQLException("Migration process timed out [" + schemaName + "." + tableName + "]. It took too long.");
        }
        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Table Manager [" + schemaName + "." + tableName + "] officially migrated.");

        /*
         * Confirm the new System Tables location by updating all local state.
         */
        oldTableManager = newTableManager;

        try {
            db.getSystemTableReference().getSystemTable().changeTableManagerLocation(newTableManager, ti);
            db.getSystemTableReference().addProxy(ti, newTableManager);
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Table Manager migration failed [" + schemaName + "." + tableName + "].");
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
