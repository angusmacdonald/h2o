package org.h2.command.h2o;

import java.sql.SQLException;

import org.h2.command.ddl.SchemaCommand;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableMigratable;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * Represents the DROP REPLICA command, allowing individual replicas to be dropped.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DropReplica extends SchemaCommand {

    // Mix of DropTable and CreateReplica code - the latter needed for the
    // 'push' functionality.

    private boolean ifExists;

    private String tableName;

    private DropReplica next;

    public DropReplica(final Session session, final Schema schema) {

        super(session, schema);
    }

    /**
     * Chain another drop table statement to this statement.
     * 
     * @param drop
     *            the statement to add
     */
    public void addNextDropTable(final DropReplica drop) {

        if (next == null) {
            next = drop;
        }
        else {
            next.addNextDropTable(drop);
        }
    }

    public void setIfExists(final boolean b) {

        ifExists = b;
        if (next != null) {
            next.setIfExists(b);
        }
    }

    public void setTableName(final String tableName) {

        this.tableName = tableName;
    }

    private void prepareDrop() throws SQLException {

        table = getSchema().findLocalTableOrView(session, tableName);

        if (table == null) {
            if (!ifExists) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName); }
        }
        else {
            session.getUser().checkRight(table, Right.ALL);
            if (!table.canDrop()) { // H2O - ensure schema tables aren't
                                    // dropped.
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
            }

            if (!table.getName().startsWith("H2O_")) {

                int numberOfReplicas = 0;

                try {
                    numberOfReplicas = session.getDatabase().getSystemTable().lookup(new TableInfo(tableName, getSchema().getName())).getTableManager().getNumberofReplicas();
                }
                catch (final RPCException e) {
                    throw new SQLException("Failed in communication with the System Table.");
                }
                catch (final MovedException e) {
                    throw new SQLException("System Table has moved.");
                }

                if (numberOfReplicas == 1) { // can't drop the only replica.
                    throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
                }

            }

            table.lock(session, true, true);
        }
        if (next != null) {
            next.prepareDrop();
        }
    }

    private void executeDrop() throws SQLException {

        // need to get the table again, because it may be dropped already
        // meanwhile (dependent object, or same object)
        table = getSchema().findLocalTableOrView(session, tableName);
        if (table != null) {
            table.setModified();
            final Database db = session.getDatabase();
            db.removeSchemaObject(session, table);

            /*
             * ################################################################## ####### Remove any System Table entries.
             * ################################################################## #######
             */
            if (!db.isManagementDB() && !db.isTableLocal(getSchema())) {
                final ISystemTableMigratable sm = db.getSystemTable(); // db.getSystemSession()

                final TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), 0, table.getTableType(), db.getID());

                try {
                    final ITableManagerRemote tmr = sm.lookup(ti).getTableManager();
                    tmr.removeReplicaInformation(ti);
                }
                catch (final RPCException e) {
                    throw new SQLException("Failed to remove replica on System Table/Table Manager");
                }
                catch (final MovedException e) {
                    throw new SQLException("System Table has moved.");
                }
            }
            H2OEventBus.publish(new H2OEvent(session.getDatabase().getID().getURL(), DatabaseStates.REPLICA_DELETION, getSchema().getName() + "." + tableName));
        }
        if (next != null) {
            next.executeDrop();
        }
    }

    @Override
    public int update() throws SQLException {

        session.commit(true);
        prepareDrop();
        executeDrop();
        return 0;
    }

}
