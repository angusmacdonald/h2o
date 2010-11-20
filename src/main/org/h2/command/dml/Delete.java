/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.log.UndoLogRecord;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement DELETE
 */
public class Delete extends Prepared {

    private Expression condition;

    private TableFilter tableFilter;

    public Delete(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    public void setTableFilter(final TableFilter tableFilter) {

        this.tableFilter = tableFilter;
        table = tableFilter.getTable();
    }

    public void setCondition(final Expression condition) {

        this.condition = condition;
    }

    @Override
    public int update() throws SQLException {

        return update(null);
    }

    @Override
    public int update(final String transactionName) throws SQLException {

        tableFilter.startQuery(session);
        tableFilter.reset();
        final Table table = tableFilter.getTable();
        setTable(table);
        session.getUser().checkRight(table, Right.DELETE);

        /*
         * (QUERY PROPAGATED TO ALL REPLICAS).
         */
        if (isRegularTable() && (tableProxy.getNumberOfReplicas() > 1 || !isReplicaLocal(tableProxy))) {
            if (tableProxy == null) { throw new SQLException("Internal Error: Query Proxy was null."); }

            String sql = null;
            if (isPreparedStatement()) {
                sql = adjustForPreparedStatement();
            }
            else {
                sql = sqlStatement;
            }

            return tableProxy.executeUpdate(sql, transactionName, session);
        }

        table.fireBefore(session);
        table.lock(session, true, false);
        final RowList rows = new RowList(session);
        try {
            setCurrentRowNumber(0);
            while (tableFilter.next()) {
                checkCanceled();
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                    final Row row = tableFilter.get();
                    if (table.fireRow()) {
                        table.fireBeforeRow(session, row, null);
                    }
                    rows.add(row);
                }
            }
            for (rows.reset(); rows.hasNext();) {
                checkCanceled();
                final Row row = rows.next();
                table.removeRow(session, row);
                session.log(table, UndoLogRecord.DELETE, row);
            }
            if (table.fireRow()) {
                for (rows.reset(); rows.hasNext();) {
                    final Row row = rows.next();
                    table.fireAfterRow(session, row, null);
                }
            }
            table.fireAfter(session);
            return rows.size();
        }
        finally {
            rows.close();
        }
    }

    private String adjustForPreparedStatement() throws SQLException {

        final List<String> values = new ArrayList<String>(); //Will be used to store the values contained within { } brackets in the final statement.

        recurisvelyEvaluateExpressionsForPreparedStatements(null, values, condition);

        // Edit the SQL String
        // Example: update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        String sql = new String(sqlStatement) + " {";

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

        return sql;
    }

    @Override
    public String getPlanSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("DELETE FROM ");
        buff.append(tableFilter.getPlanSQL(false));
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

        acquireLocks(tableProxyManager, table, LockType.WRITE);

    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#shouldBePropagated()
     */
    @Override
    public boolean shouldBePropagated() {

        /*
         * If this is not a regular table (i.e. it is a meta-data table, then it will not be propagated regardless.
         */
        return isRegularTable();
    }

}
