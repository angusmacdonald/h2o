/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * This class represents the statement DROP TABLE
 */
public class DropTable extends SchemaCommand {

    private boolean ifExists;

    private String tableName;

    ReplicaSet tables = null;

    private DropTable next;

    public DropTable(final Session session, final Schema schema, final boolean internalQuery) {

        super(session, schema);

        this.internalQuery = internalQuery;
    }

    /**
     * Chain another drop table statement to this statement.
     * 
     * @param drop
     *            the statement to add
     */
    public void addNextDropTable(final DropTable drop) {

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

    private void prepareDrop(final String transactionName) throws SQLException {

        tables = getSchema().getTablesOrViews(session, tableName);

        if (tables != null) {
            table = tables.getACopy();
        }

        TableManagerRemote tableManager = null;
        if (table == null) {
            tableManager = getSchema().getDatabase().getSystemTableReference().lookup((getSchema().getName() + "." + tableName), false);
        }

        if (table == null && tableManager == null) {
            // table = null;
            if (!ifExists) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName); }
        }
        else {
            session.getUser().checkRight(table, Right.ALL);

            if (table != null && !table.canDrop() || tableName.startsWith("H2O_")) { // H2O  - ensure schema tables aren't dropped.
                throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
            }
            if (!internalQuery && table != null) {
                table.lock(session, true, true); // lock isn't acquired here - the query is distributed to each replica first.
            }
        }
        if (next != null) {
            next.prepareDrop(transactionName);
        }
    }

    private void executeDrop(final String transactionName) throws SQLException, RemoteException {

        // need to get the table again, because it may be dropped already in the meantime (dependent object, or same object)
        table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);
        if (table != null) {
            final Database db = session.getDatabase();

            if (!db.isManagementDB() && !db.isTableLocal(getSchema()) && !internalQuery) {

                tableProxy.executeUpdate(sqlStatement, transactionName, session);

                final ISystemTableReference sm = db.getSystemTableReference();
                try {
                    sm.removeTableInformation(new TableInfo(tableName, getSchema().getName()));
                }
                catch (final MovedException e) {
                    db.getSystemTableReference().handleMovedException(e);

                    try {
                        sm.removeTableInformation(new TableInfo(tableName, getSchema().getName()));
                    }
                    catch (final MovedException e1) {
                        throw new RemoteException("System Table has moved.");
                    }
                }

            }
            else { // It's an internal query...

                /*
                 * We want to remove the local copy of the data plus any other references to remote tables (i.e. linked tables need to be
                 * removed as well)
                 */
                final Table[] tableArray = tables.getAllCopies().toArray(new Table[0]); // add to array to prevent concurrent
                // modification exceptions.

                for (final Table t : tableArray) {
                    t.setModified();
                    db.removeSchemaObject(session, t);
                }

                // db.removeTableManager(fullTableName, true);

            }
            H2OEventBus.publish(new H2OEvent(db.getURL().getURL(), DatabaseStates.TABLE_DELETION, getSchema().getName() + "." + tableName));
        }
        if (next != null) {
            next.executeDrop(transactionName);
        }
    }

    @Override
    public int update(final String transactionName) throws SQLException, RemoteException {

        session.commit(true);
        prepareDrop(transactionName);
        executeDrop(transactionName);
        return 0;
    }

    @Override
    public int update() throws SQLException, RemoteException {

        final String transactionName = "None";

        session.commit(true);
        prepareDrop(transactionName);
        executeDrop(transactionName);
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#acquireLocks()
     */
    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        /*
         * (QUERY PROPAGATED TO ALL REPLICAS).
         */
        if (isRegularTable()) {

            final String fullTableName = getSchema().getName() + "." + tableName;
            tableProxy = tableProxyManager.getQueryProxy(fullTableName);

            if (tableProxy == null || !tableProxy.getLockGranted().equals(LockType.WRITE)) {

                final Database database = getSchema().getDatabase();
                final TableManagerRemote tableManager = database.getSystemTableReference().lookup(fullTableName, true);

                if (tableManager == null) {
                    // Will happen if the table doesn't exist but IF NOT EXISTS has been specified.
                    tableProxy = TableProxy.getDummyQueryProxy(LockRequest.createNewLockRequest(session));
                }
                else {
                    /*
                     * A DROP lock is requested if auto-commit is off (so that the update ID returned is 0), but a WRITE lock is given. If
                     * auto-commit is on then no other queries can come in after the drop request so the write lock is sufficient.
                     */

                    final LockType lockToRequest = session.getApplicationAutoCommit() ? LockType.WRITE : LockType.DROP;

                    tableProxy = TableProxy.getQueryProxyAndLock(tableManager, fullTableName, LockRequest.createNewLockRequest(session), lockToRequest, database, false);
                }
            }
            tableProxyManager.addProxy(tableProxy);
        }
        else {
            tableProxyManager.addProxy(TableProxy.getDummyQueryProxy(LockRequest.createNewLockRequest(session)));
        }

    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#isRegularTable()
     */
    @Override
    protected boolean isRegularTable() {

        final Set<String> localSchema = session.getDatabase().getLocalSchema();
        try {
            return !session.getDatabase().isManagementDB() && !internalQuery && !localSchema.contains(getSchema().getName());
        }
        catch (final NullPointerException e) {
            // Shouldn't occur, ever. Something should have probably overridden
            // this if it can't possibly know about a particular table.
            ErrorHandling.hardError("isRegularTable() check failed.");
            return false;
        }
    }

}
