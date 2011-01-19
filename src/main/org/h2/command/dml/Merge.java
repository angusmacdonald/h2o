/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * This class represents the statement MERGE
 */
public class Merge extends Prepared {

    private Column[] columns;

    private Column[] keys;

    private final ObjectArray list = new ObjectArray();

    private Query query;

    private Prepared update;

    public Merge(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
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

    public void setKeys(final Column[] keys) {

        this.keys = keys;
    }

    public void setQuery(final Query query) {

        this.query = query;
    }

    /**
     * Add a row to this merge statement.
     * 
     * @param expr
     *            the list of values
     */
    public void addRow(final Expression[] expr) {

        list.add(expr);
    }

    @Override
    public int update() throws SQLException, RPCException {

        int count;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        if (keys == null) {
            final Index idx = table.getPrimaryKey();
            if (idx == null) { throw Message.getSQLException(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY"); }
            keys = idx.getColumns();
        }
        final StringBuilder buff = new StringBuilder("UPDATE ");
        buff.append(table.getSQL());
        buff.append(" SET ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
            buff.append("=?");
        }
        buff.append(" WHERE ");
        for (int i = 0; i < keys.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append(keys[i].getSQL());
            buff.append("=?");
        }
        final String sql = buff.toString();
        update = session.prepare(sql);
        setCurrentRowNumber(0);
        if (list.size() > 0) {
            count = 0;
            for (int x = 0; x < list.size(); x++) {
                setCurrentRowNumber(x + 1);
                final Expression[] expr = (Expression[]) list.get(x);
                final Row newRow = table.getTemplateRow();
                for (int i = 0; i < columns.length; i++) {
                    final Column c = columns[i];
                    final int index = c.getColumnId();
                    final Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        try {
                            final Value v = expr[i].getValue(session).convertTo(c.getType());
                            newRow.setValue(index, v);
                        }
                        catch (final SQLException ex) {
                            throw setRow(ex, count, getSQL(expr));
                        }
                    }
                }
                merge(newRow);
                count++;
            }
        }
        else {
            final LocalResult rows = query.query(0);
            count = 0;
            table.fireBefore(session);
            table.lock(session, true, false);
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
                merge(newRow);
            }
            rows.close();
            table.fireAfter(session);
        }
        return count;
    }

    private void merge(final Row row) throws SQLException, RPCException {

        final ObjectArray k = update.getParameters();
        for (int i = 0; i < columns.length; i++) {
            final Column col = columns[i];
            final Value v = row.getValue(col.getColumnId());
            final Parameter p = (Parameter) k.get(i);
            p.setValue(v);
        }
        for (int i = 0; i < keys.length; i++) {
            final Column col = keys[i];
            final Value v = row.getValue(col.getColumnId());
            if (v == null) { throw Message.getSQLException(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL()); }
            final Parameter p = (Parameter) k.get(columns.length + i);
            p.setValue(v);
        }
        final int count = update.update("Merge");
        if (count == 0) {
            table.fireBefore(session);
            table.validateConvertUpdateSequence(session, row);
            table.fireBeforeRow(session, null, row);
            table.lock(session, true, false);
            table.addRow(session, row);
            session.log(table, UndoLogRecord.INSERT, row);
            table.fireAfter(session);
            table.fireAfterRow(session, null, row);
        }
        else if (count != 1) { throw Message.getSQLException(ErrorCode.DUPLICATE_KEY_1, table.getSQL()); }
    }

    @Override
    public String getPlanSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("MERGE INTO ");
        buff.append(table.getSQL());
        buff.append('(');
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")");
        if (keys != null) {
            buff.append(" KEY(");
            for (int i = 0; i < keys.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(keys[i].getSQL());
            }
            buff.append(")");
        }
        buff.append('\n');
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
                    final Expression e = expr[i];
                    if (e != null) {
                        expr[i] = e.optimize(session);
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

}
