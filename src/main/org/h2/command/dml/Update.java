/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement UPDATE
 */
public class Update extends Prepared {

    private Expression condition;

    private TableFilter tableFilter;

    private Expression[] expressions;

    public Update(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    public void setTableFilter(final TableFilter tableFilter) {

        this.tableFilter = tableFilter;
        final Table table = tableFilter.getTable();
        expressions = new Expression[table.getColumns().length];
    }

    public void setCondition(final Expression condition) {

        this.condition = condition;
    }

    /**
     * Add an assignment of the form column = expression.
     * 
     * @param column
     *            the column
     * @param expression
     *            the expression
     */
    public void setAssignment(final Column column, final Expression expression) throws SQLException {

        final int id = column.getColumnId();
        if (expressions[id] != null) { throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName()); }
        expressions[id] = expression;
        if (expression instanceof Parameter) {
            final Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    @Override
    public int update(final String transactionName) throws SQLException {

        tableFilter.startQuery(session);
        tableFilter.reset();
        final RowList rows = new RowList(session);
        try {
            final Table table = tableFilter.getTable();
            setTable(table);
            session.getUser().checkRight(table, Right.UPDATE);

            /*
             * (QUERY PROPAGATED TO ALL REPLICAS).
             */
            if (isRegularTable() && thisIsNotALocalOrSingleTableUpdate()) {

                String sql = null;
                if (isPreparedStatement()) {
                    sql = adjustForPreparedStatement();
                }
                else {
                    sql = sqlStatement;
                }

                if (tableProxy == null) {
                    tableProxy = new TableProxy(new LockRequest(session)); // in case of MERGE statement.
                }
                return tableProxy.executeUpdate(sql, transactionName, session);
            }

            table.fireBefore(session);
            table.lock(session, true, false);
            final int columnCount = table.getColumns().length;
            // get the old rows, compute the new rows
            setCurrentRowNumber(0);
            int count = 0;
            while (tableFilter.next()) {
                checkCanceled();
                setCurrentRowNumber(count + 1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    final Row oldRow = tableFilter.get();
                    final Row newRow = table.getTemplateRow();
                    for (int i = 0; i < columnCount; i++) {
                        final Expression newExpr = expressions[i];
                        Value newValue;
                        if (newExpr == null) {
                            newValue = oldRow.getValue(i);
                        }
                        else if (newExpr == ValueExpression.getDefault()) {
                            final Column column = table.getColumn(i);
                            final Expression defaultExpr = column.getDefaultExpression();
                            Value v;
                            if (defaultExpr == null) {
                                v = column.validateConvertUpdateSequence(session, null);
                            }
                            else {
                                v = defaultExpr.getValue(session);
                            }
                            final int type = column.getType();
                            newValue = v.convertTo(type);
                        }
                        else {
                            final Column column = table.getColumn(i);
                            newValue = newExpr.getValue(session).convertTo(column.getType());
                        }
                        newRow.setValue(i, newValue);
                    }
                    table.validateConvertUpdateSequence(session, newRow);
                    if (table.fireRow()) {
                        table.fireBeforeRow(session, oldRow, newRow);
                    }
                    rows.add(oldRow);
                    rows.add(newRow);
                    count++;
                }
            }
            // TODO self referencing referential integrity constraints
            // don't work if update is multi-row and 'inversed' the condition!
            // probably need multi-row triggers with 'deleted' and 'inserted'
            // at the same time. anyway good for sql compatibility
            // TODO update in-place (but if the position changes,
            // we need to update all indexes) before row triggers

            // the cached row is already updated - we need the old values
            table.updateRows(this, session, rows);
            if (table.fireRow()) {
                rows.invalidateCache();
                for (rows.reset(); rows.hasNext();) {
                    checkCanceled();
                    final Row o = rows.next();
                    final Row n = rows.next();
                    table.fireAfterRow(session, o, n);
                }
            }
            table.fireAfter(session);
            return count;
        }
        finally {
            rows.close();
        }
    }

    /**
     * Returns true if this update involves a query proxy where there are multiple replicas.
     * @return
     */
    private boolean thisIsNotALocalOrSingleTableUpdate() {

        final boolean multipleReplicas = tableProxy.getNumberOfReplicas() > 1;
        final boolean notLocal = !isReplicaLocal(tableProxy);

        return tableProxy != null && multipleReplicas || notLocal;
    }

    /**
     * Adjusts the sqlStatement string to be a valid prepared statement. This is used when propagating prepared statements within the
     * system.
     * 
     * This method takes the parsed SQL and extracts the values needed to pass information to the second machine.
     * @return The SQL update string combined with prepared statement values at the end. 
     * Example: update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
     * @throws SQLException
     */
    private String adjustForPreparedStatement() throws SQLException {

        String sql = null;
        try {

            // Adjust the sqlStatement string to contain actual data.

            final Expression[] expr = expressions;

            final String[] values = new String[org.h2o.util.StringUtils.countNumberOfCharacters(sqlStatement, "?")]; //Will be used to store the values contained within { } brackets in the final statement.

            Table localTableReference = table;

            if (table == null) {
                localTableReference = tableFilter.getTable();
            }

            final Column[] columns = localTableReference.getColumns();

            /*
             * 'Expressions' stores all of the expressions being set by this update. The expression will be null if nothing is being set.
             * 'Condition' stores the set condition.
             */
            for (int i = 0; i < expressions.length; i++) {
                //Loop through all of the expressions that are part of the UPDATE clause (not part of the WHERE clause).
                final Column c = columns[i];
                final int index = c.getColumnId();
                final Expression e = expr[i];

                evaluateExpressionForPreparedStatement(e, values, c.getType(), expr);
            }

            recurisvelyEvaluateExpressionsForPreparedStatements(expr, values, condition);

            // Edit the SQL String (add data to the end)
            // Example: update bahrain set Name=? where ID=? {1: 'PILOT_1', 2:
            // 1};
            sql = new String(sqlStatement) + " {";

            boolean addComma = false;
            int count = 1;
            for (final String value : values) {
                if (value != null) {
                    if (addComma) {
                        sql += ", ";
                    }
                    else {
                        addComma = true;
                    }

                    sql += count + ": " + value;

                    count++;
                }
            }
            sql += "};";
        }
        catch (final Exception e) {
            e.printStackTrace();
            throw new SQLException(e.getMessage());
        }
        return sql;
    }

    @Override
    public String getPlanSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("UPDATE ");
        buff.append(tableFilter.getPlanSQL(false));
        buff.append("\nSET ");
        final Table table = tableFilter.getTable();
        final int columnCount = table.getColumns().length;
        for (int i = 0, j = 0; i < columnCount; i++) {
            final Expression newExpr = expressions[i];
            if (newExpr != null) {
                if (j > 0) {
                    buff.append(",\n");
                }
                j++;
                final Column column = table.getColumn(i);
                buff.append(column.getName());
                buff.append(" = ");
                buff.append(newExpr.getSQL());
            }
        }
        if (condition != null) {
            buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
        }
        return buff.toString();
    }

    @Override
    public void prepare() throws SQLException {

        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
        }
        for (int i = 0; i < expressions.length; i++) {
            final Expression expr = expressions[i];
            if (expr != null) {
                expr.mapColumns(tableFilter, 0);
                expressions[i] = expr.optimize(session);
            }
        }
        final PlanItem item = tableFilter.getBestPlanItem(session);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    @Override
    public boolean isTransactional() {

        return true;
    }

    @Override
    public LocalResult queryMeta() {

        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#acquireLocks()
     */
    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        acquireLocks(tableProxyManager, tableFilter.getTable(), LockType.WRITE);

    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#isRegularTable()
     */
    @Override
    protected boolean isRegularTable() {

        final boolean isLocal = session.getDatabase().isTableLocal(tableFilter.getTable().getSchema());
        return !session.getDatabase().isManagementDB() && !internalQuery && !isLocal;
    }

    @Override
    public String getSQLIncludingParameters() {

        if (isPreparedStatement()) {
            try {
                return adjustForPreparedStatement();
            }
            catch (final Exception e) {
                e.printStackTrace();
                return toString();
            }
        }
        else {
            return toString();
        }
    }
}
