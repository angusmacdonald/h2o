/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableLink;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * A linked index is a index for a linked (remote) table. It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends BaseIndex {

    private final TableLink link;

    private final String targetTableName;

    private long rowCount;

    public LinkedIndex(final TableLink table, final int id, final IndexColumn[] columns, final IndexType indexType) {

        initBaseIndex(table, id, null, columns, indexType);
        link = table;
        targetTableName = link.getQualifiedTable();
    }

    @Override
    public String getCreateSQL() {

        return null;
    }

    @Override
    public void close(final Session session) {

        // nothing to do
    }

    private boolean isNull(final Value v) {

        return v == null || v == ValueNull.INSTANCE;
    }

    @Override
    public void add(final Session session, final Row row) throws SQLException {

        final StringBuilder buff = new StringBuilder("INSERT INTO ");
        buff.append(targetTableName);
        buff.append(" VALUES(");
        for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
            final Value v = row.getValue(i);
            if (j > 0) {
                buff.append(',');
            }
            j++;
            if (isNull(v)) {
                buff.append("NULL");
            }
            else {
                buff.append('?');
            }
        }
        buff.append(')');
        final String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                final PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
                    final Value v = row.getValue(i);
                    if (v != null && v != ValueNull.INSTANCE) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                prep.executeUpdate();
                rowCount++;
            }
            catch (final SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    @Override
    public Cursor find(final Session session, final SearchRow first, final SearchRow last) throws SQLException {

        final StringBuilder buff = new StringBuilder();
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            final Value v = first.getValue(i);
            if (v != null) {
                if (buff.length() != 0) {
                    buff.append(" AND ");
                }
                final Column col = table.getColumn(i);
                buff.append(col.getSQL());
                buff.append(">=");
                addParameter(buff, col);
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            final Value v = last.getValue(i);
            if (v != null) {
                if (buff.length() != 0) {
                    buff.append(" AND ");
                }
                final Column col = table.getColumn(i);
                buff.append(col.getSQL());
                buff.append("<=");
                addParameter(buff, col);
            }
        }
        if (buff.length() > 0) {
            buff.insert(0, " WHERE ");
        }
        buff.insert(0, "SELECT * FROM " + targetTableName + " T");
        final String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                final PreparedStatement prep = link.getPreparedStatement(sql + "[internal]", true);
                int j = 0;
                for (int i = 0; first != null && i < first.getColumnCount(); i++) {
                    final Value v = first.getValue(i);
                    if (v != null) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                for (int i = 0; last != null && i < last.getColumnCount(); i++) {
                    final Value v = last.getValue(i);
                    if (v != null) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                final ResultSet rs = prep.executeQuery();
                return new LinkedCursor(link, rs, session, sql, prep);
            }
            catch (final SQLException e) {
                ErrorHandling.exceptionError(e, "Failed to connect via linked table to table on " + link.getUrl());
                throw link.wrapException(sql, e);
            }
        }
    }

    private void addParameter(final StringBuilder buff, final Column col) {

        if (col.getType() == Value.STRING_FIXED && link.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            buff.append("CAST(? AS CHAR(");
            buff.append(col.getPrecision());
            buff.append("))");
        }
        else {
            buff.append("?");
        }
    }

    @Override
    public double getCost(final Session session, final int[] masks) {

        return 100 + getCostRangeIndex(masks, rowCount + Constants.COST_ROW_OFFSET);
    }

    @Override
    public void remove(final Session session) {

        // nothing to do
    }

    @Override
    public void truncate(final Session session) {

        // nothing to do
    }

    @Override
    public void checkRename() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public boolean needRebuild() {

        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {

        return false;
    }

    @Override
    public Cursor findFirstOrLast(final Session session, final boolean first) throws SQLException {

        // TODO optimization: could get the first or last value (in any case;
        // maybe not optimized)
        throw Message.getUnsupportedException();
    }

    @Override
    public void remove(final Session session, final Row row) throws SQLException {

        final StringBuilder buff = new StringBuilder("DELETE FROM ");
        buff.append(targetTableName);
        buff.append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            if (i > 0) {
                buff.append("AND ");
            }
            final Column col = table.getColumn(i);
            buff.append(col.getSQL());
            final Value v = row.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            }
            else {
                buff.append('=');
                addParameter(buff, col);
                buff.append(' ');
            }
        }
        final String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                final PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0, j = 0; i < row.getColumnCount(); i++) {
                    final Value v = row.getValue(i);
                    if (!isNull(v)) {
                        v.set(prep, j + 1);
                        j++;
                    }
                }
                final int count = prep.executeUpdate();
                rowCount -= count;
            }
            catch (final SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the emit updates option is enabled.
     * 
     * @param oldRow
     *            the old data
     * @param newRow
     *            the new data
     */
    public void update(final Row oldRow, final Row newRow) throws SQLException {

        final StringBuilder buff = new StringBuilder("UPDATE ");
        buff.append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(table.getColumn(i).getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            if (i > 0) {
                buff.append("AND ");
            }
            final Column col = table.getColumn(i);
            buff.append(col.getSQL());
            final Value v = oldRow.getValue(i);
            if (isNull(v)) {
                buff.append(" IS NULL ");
            }
            else {
                buff.append('=');
                addParameter(buff, col);
                buff.append(' ');
            }
        }
        final String sql = buff.toString();
        synchronized (link.getConnection()) {
            try {
                int j = 1;
                final PreparedStatement prep = link.getPreparedStatement(sql, false);
                for (int i = 0; i < newRow.getColumnCount(); i++) {
                    newRow.getValue(i).set(prep, j);
                    j++;
                }
                for (int i = 0; i < oldRow.getColumnCount(); i++) {
                    final Value v = oldRow.getValue(i);
                    if (!isNull(v)) {
                        v.set(prep, j);
                        j++;
                    }
                }
                final int count = prep.executeUpdate();
                // this has no effect but at least it allows to debug the update
                // count
                rowCount = rowCount + count - count;
            }
            catch (final SQLException e) {
                throw link.wrapException(sql, e);
            }
        }
    }

    @Override
    public long getRowCount(final Session session) {

        return rowCount;
    }

    @Override
    public long getRowCountApproximation() {

        return rowCount;
    }

}
