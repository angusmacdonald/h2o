/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.FunctionCall;
import org.h2.expression.TableFunction;
import org.h2.index.FunctionIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * A table backed by a system or user-defined function that returns a result set.
 */
public class FunctionTable extends Table {

    private final FunctionCall function;

    private final long rowCount;

    private Expression functionExpr;

    private LocalResult cachedResult;

    private Value cachedValue;

    public FunctionTable(final Schema schema, final Session session, final Expression functionExpr, final FunctionCall function) throws SQLException {

        super(schema, 0, function.getName(), false);
        this.functionExpr = functionExpr;
        this.function = function;
        if (function instanceof TableFunction) {
            rowCount = ((TableFunction) function).getRowCount();
        }
        else {
            rowCount = Long.MAX_VALUE;
        }
        function.optimize(session);
        final int type = function.getType();
        if (type != Value.RESULT_SET) { throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName()); }
        final int params = function.getParameterCount();
        final Expression[] columnListArgs = new Expression[params];
        final Expression[] args = function.getArgs();
        for (int i = 0; i < params; i++) {
            args[i] = args[i].optimize(session);
            columnListArgs[i] = args[i];
        }
        final ValueResultSet template = function.getValueForColumnList(session, columnListArgs);
        if (template == null) { throw Message.getSQLException(ErrorCode.FUNCTION_MUST_RETURN_RESULT_SET_1, function.getName()); }
        final ResultSet rs = template.getResultSet();
        final ResultSetMetaData meta = rs.getMetaData();
        final int columnCount = meta.getColumnCount();
        final Column[] cols = new Column[columnCount];
        for (int i = 0; i < columnCount; i++) {
            cols[i] = new Column(meta.getColumnName(i + 1), DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1)), meta.getPrecision(i + 1), meta.getScale(i + 1), meta.getColumnDisplaySize(i + 1));
        }
        setColumns(cols);
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
    boolean isLockedExclusivelyBy(final Session session) {

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
    public void truncate(final Session session) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public boolean canDrop() {

        throw Message.throwInternalError();
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
    public String getTableType() {

        return null;
    }

    @Override
    public Index getScanIndex(final Session session) {

        return new FunctionIndex(this, IndexColumn.wrap(columns));
    }

    @Override
    public ObjectArray getIndexes() {

        return null;
    }

    @Override
    public boolean canGetRowCount() {

        return rowCount != Long.MAX_VALUE;
    }

    @Override
    public long getRowCount(final Session session) {

        return rowCount;
    }

    @Override
    public String getCreateSQL() {

        return null;
    }

    @Override
    public String getDropSQL() {

        return null;
    }

    @Override
    public void checkRename() throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Read the result set from the function.
     * 
     * @param session
     *            the session
     * @return the result set
     */
    public LocalResult getResult(final Session session) throws SQLException {

        functionExpr = functionExpr.optimize(session);
        final Value v = functionExpr.getValue(session);
        if (cachedResult != null && cachedValue == v) {
            cachedResult.reset();
            return cachedResult;
        }
        if (v == ValueNull.INSTANCE) { return new LocalResult(); }
        final ValueResultSet value = (ValueResultSet) v;
        final ResultSet rs = value.getResultSet();
        final LocalResult result = LocalResult.read(session, rs, 0);
        if (function.isDeterministic()) {
            cachedResult = result;
            cachedValue = v;
        }
        return result;
    }

    @Override
    public long getMaxDataModificationId() {

        // TODO optimization: table-as-a-function currently doesn't know the
        // last modified date
        return Long.MAX_VALUE;
    }

    @Override
    public Index getUniqueIndex() {

        return null;
    }

    @Override
    public String getSQL() {

        return function.getSQL();
    }

    @Override
    public long getRowCountApproximation() {

        return rowCount;
    }

    @Override
    public boolean isLocal() {

        return true;
    }
}
