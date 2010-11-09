/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.RangeIndex;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * The table SYSTEM_RANGE is a virtual table that generates incrementing numbers with a given start end end point.
 */
public class RangeTable extends Table {

    /**
     * The name of the range table.
     */
    public static final String NAME = "SYSTEM_RANGE";

    private Expression min, max;

    private boolean optimized;

    /**
     * Create a new range with the given start and end expressions.
     * 
     * @param schema
     *            the schema (always the main schema)
     * @param min
     *            the start expression
     * @param max
     *            the end expression
     */
    public RangeTable(final Schema schema, final Expression min, final Expression max) throws SQLException {

        super(schema, 0, NAME, true);
        final Column[] cols = new Column[]{new Column("X", Value.LONG)};
        this.min = min;
        this.max = max;
        setColumns(cols);
    }

    @Override
    public String getDropSQL() {

        return null;
    }

    @Override
    public String getCreateSQL() {

        return null;
    }

    @Override
    public String getSQL() {

        return NAME + "(" + min.getSQL() + ", " + max.getSQL() + ")";
    }

    @Override
    public Session lock(final Session session, final boolean exclusive, final boolean force) {

        return null;
        // nothing to do
    }

    @Override
    public void close(final Session session) {

        // nothing to do
    }

    @Override
    public void unlock(final Session s) {

        // nothing to do
    }

    @Override
    public boolean isLockedExclusively() {

        return false;
    }

    @Override
    public Index addIndex(final Session session, final String indexName, final int indexId, final IndexColumn[] cols, final IndexType indexType, final int headPos, final String comment) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void removeRow(final Session session, final Row row) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void addRow(final Session session, final Row row) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void checkSupportAlter() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void checkRename() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public boolean canGetRowCount() {

        return true;
    }

    @Override
    public boolean canDrop() {

        return false;
    }

    @Override
    public long getRowCount(final Session session) throws SQLException {

        return getMax(session) - getMin(session);
    }

    @Override
    public String getTableType() {

        return "RANGE_TABLE";
    }

    @Override
    public Index getScanIndex(final Session session) {

        return new RangeIndex(this, IndexColumn.wrap(columns));
    }

    /**
     * Calculate and get the start value of this range.
     * 
     * @param session
     *            the session
     * @return the start value
     */
    public long getMin(final Session session) throws SQLException {

        optimize(session);
        return min.getValue(session).getLong();
    }

    /**
     * Calculate and get the end value of this range.
     * 
     * @param session
     *            the session
     * @return the end value
     */
    public long getMax(final Session session) throws SQLException {

        optimize(session);
        return max.getValue(session).getLong();
    }

    private void optimize(final Session s) throws SQLException {

        if (!optimized) {
            min = min.optimize(s);
            max = max.optimize(s);
            optimized = true;
        }
    }

    @Override
    public ObjectArray getIndexes() {

        return null;
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public long getMaxDataModificationId() {

        return 0;
    }

    @Override
    public Index getUniqueIndex() {

        return null;
    }

    @Override
    public long getRowCountApproximation() {

        return 100;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#isLocal()
     */
    @Override
    public boolean isLocal() {

        return true;
    }
}
