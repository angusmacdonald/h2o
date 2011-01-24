/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.h2.command.Command;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2o.db.id.TableInfo;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.asynchronous.CommitResult;
import org.h2o.db.query.locking.LockType;
import org.h2o.test.AsynchronousTests;

/**
 * This class represents the statement INSERT
 */
public class Insert extends Prepared {

    private Column[] columns;

    private final ObjectArray list = new ObjectArray();

    private Query query;

    /**
     * 
     * @param session
     *            The current session.
     * @param internalQuery
     *            True if this query has been sent internally through the RMI interface, false if it has come from an external JBDC
     *            connection.
     */
    public Insert(final Session session, final boolean internalQuery) {

        super(session, internalQuery);

        this.internalQuery = internalQuery;
    }

    @Override
    public void setCommand(final Command command) {

        super.setCommand(command);
        if (query != null) {
            query.setCommand(command);
        }
    }

    public void setColumns(final Column[] columns) {

        this.columns = columns;
    }

    public void setQuery(final Query query) {

        this.query = query;
    }

    /**
     * Add a row to this merge statement.
     * 
     * @param expr the list of values
     */
    public void addRow(final Expression[] expr) {

        list.add(expr);
    }

    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        acquireLocks(tableProxyManager, table, LockType.WRITE);
    }

    @Override
    public int update() throws SQLException {

        return update(null);
    }

    @Override
    public int update(final String transactionName) throws SQLException {

        int count = 0;

        session.getUser().checkRight(table, Right.INSERT);

        /*
         * (QUERY PROPAGATED TO ALL REPLICAS).
         */
        if (isRegularTable() && (tableProxy.getNumberOfReplicas() > 1 || !isReplicaLocal(tableProxy))) {
            String sql;

            if (isPreparedStatement()) {
                sql = adjustForPreparedStatement();
            }
            else {
                sql = sqlStatement;
            }

            return tableProxy.executeUpdate(sql, transactionName, session);
        }
        else if (isRegularTable()) {
            /*
             * If this is going to be an entirely local query we still need to create a record of it in case it is part of a larger transaction.
             */
            final List<CommitResult> recentlyCompletedQueries = new LinkedList<CommitResult>();
            recentlyCompletedQueries.add(new CommitResult(true, session.getDatabase().getLocalDatabaseInstanceInWrapper(), tableProxy.getUpdateID(), tableProxy.getUpdateID(), new TableInfo(table.getFullName())));
            session.getDatabase().getAsynchronousQueryManager().addTransaction(transactionName, new TableInfo(table.getFullName()), null, recentlyCompletedQueries, 0);
        }

        AsynchronousTests.pauseThreadIfTestingAsynchronousUpdates(table, session.getDatabase().getDatabaseSettings(), session.getDatabase().getID(), getSQL());

        setCurrentRowNumber(0);
        if (list.size() > 0) {
            count = 0;
            for (int x = 0; x < list.size(); x++) {
                final Expression[] expr = (Expression[]) list.get(x);
                final Row newRow = table.getTemplateRow();
                setCurrentRowNumber(x + 1);
                for (int i = 0; i < columns.length; i++) {
                    final Column c = columns[i];
                    final int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        try {
                            final Value v = e.getValue(session).convertTo(c.getType());
                            newRow.setValue(index, v);

                        }
                        catch (final SQLException ex) {
                            throw setRow(ex, x, getSQL(expr));
                        }
                    }
                }
                checkCanceled();
                table.fireBefore(session);
                table.validateConvertUpdateSequence(session, newRow);
                table.fireBeforeRow(session, null, newRow);

                final Session sessionGrantedLock = table.lock(session, true, false);

                assert sessionGrantedLock == session : sessionIdentityAssertionInfo(session, sessionGrantedLock);

                table.addRow(session, newRow);

                session.log(table, UndoLogRecord.INSERT, newRow);
                table.fireAfter(session);
                table.fireAfterRow(session, null, newRow);
                count++;
            }
        }
        else {
            final LocalResult rows = query.query(0);
            count = 0;
            table.fireBefore(session);
            final Session lockSession = table.lock(session, true, false);

            assert lockSession == session : "The lock should have been taken out on the requesting session.";

            while (rows.next()) {
                checkCanceled();
                count++;
                final Value[] r = rows.currentRow();
                final Row newRow = table.getTemplateRow();
                setCurrentRowNumber(count);
                for (int j = 0; j < columns.length; j++) {
                    final Column c = columns[j];
                    final int index = c.getColumnId();
                    try {
                        final Value v = r[j].convertTo(c.getType());
                        newRow.setValue(index, v);
                    }
                    catch (final SQLException ex) {
                        throw setRow(ex, count, getSQL(r));
                    }
                }
                table.validateConvertUpdateSequence(session, newRow);
                table.fireBeforeRow(session, null, newRow);

                assert lockSession == session : "The lock should have been taken out on the requesting session.";

                table.addRow(session, newRow);

                session.log(table, UndoLogRecord.INSERT, newRow);
                table.fireAfterRow(session, null, newRow);
            }
            rows.close();
            table.fireAfter(session);
        }

        return count;
    }

    private String sessionIdentityAssertionInfo(final Session session, final Session sessionGrantedLock) {

        return "Lock requested by session: " + session + " but granted to session: " + sessionGrantedLock + " for query: " + sqlStatement;
    }

    /**
     * Adjusts the sqlStatement string to be a valid prepared statement. This is used when propagating prepared statements within the
     * system.
     * 
     * @param sql
     * @return
     * @throws SQLException
     */
    private String adjustForPreparedStatement() throws SQLException {

        // Adjust the sqlStatement string to contain actual data.

        // if this is a prepared statement the SQL must look like: insert into
        // PUBLIC.TEST (id,name) values (?,?) {1: 99, 2: 'helloNumber99'};
        // use the loop structure from below to obtain this information. how do
        // you know if it is a prepared statement.

        try {
            final Expression[] expr = (Expression[]) list.get(0);
            final String[] values = new String[columns.length];

            for (int i = 0; i < columns.length; i++) {
                final Column c = columns[i];
                Expression e = expr[i];

                // Only add the expression if it is unspecified in the query (there
                // will be an instance of parameter somewhere).
                if (e != null && e instanceof Parameter || e instanceof Operation && e.toString().contains("?")) {
                    // e can be null (DEFAULT)
                    e = e.optimize(session);
                    try {
                        final Value v = e.getValue(session).convertTo(c.getType());
                        values[i] = v.toString();
                        // newRow.setValue(index, v);
                    }
                    catch (final SQLException ex) {
                        throw setRow(ex, 0, getSQL(expr));
                    }
                }
            }

            // Edit the SQL String
            // insert into PUBLIC.TEST (id,name) values (?,?) {1: 99, 2:
            // 'helloNumber99'};

            String sql = new String(sqlStatement) + " {";

            for (int i = 1; i <= columns.length; i++) {
                if (values[i - 1] != null) {
                    if (i > 1) {
                        sql += ", ";
                    }
                    sql += i + ": " + values[i - 1];
                }
            }
            sql += "};";

            return sql;

        }
        catch (final SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public String getPlanSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("INSERT INTO ");
        buff.append(table.getSQL());
        buff.append('(');
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")\n");
        if (list.size() > 0) {
            buff.append("VALUES ");
            for (int x = 0; x < list.size(); x++) {
                final Expression[] expr = (Expression[]) list.get(x);
                if (x > 0) {
                    buff.append(", ");
                }
                buff.append("(");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        buff.append(", ");
                    }
                    final Expression e = expr[i];
                    if (e == null) {
                        buff.append("DEFAULT");
                    }
                    else {
                        buff.append(e.getSQL());
                    }
                }
                buff.append(')');
            }
        }
        else {
            buff.append(query.getPlanSQL());
        }
        return buff.toString();
    }

    @Override
    public void prepare() throws SQLException {

        if (columns == null) {
            if (list.size() > 0 && ((Expression[]) list.get(0)).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            }
            else {
                columns = table.getColumns();
            }
        }
        if (list.size() > 0) {
            for (int x = 0; x < list.size(); x++) {
                final Expression[] expr = (Expression[]) list.get(x);
                if (expr.length != columns.length) { throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH); }
                for (int i = 0; i < expr.length; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        e = e.optimize(session);
                        if (e instanceof Parameter) {
                            final Parameter p = (Parameter) e;
                            p.setColumn(columns[i]);
                        }
                        expr[i] = e;
                    }
                }
            }
        }
        else {
            query.prepare();
            if (query.getColumnCount() != columns.length) { throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH); }
        }
    }

    @Override
    public boolean isTransactional() {

        return true;
    }

    @Override
    public LocalResult queryMeta() {

        return null;
    }

    @Override
    public boolean shouldBePropagated() {

        /*
         * If this is not a regular table (i.e. it is a meta-data table, then it will not be propagated regardless.
         */
        return isRegularTable();
    }
}
