/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.constant.SysProperties;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A table filter represents a table that is used in a query. There is one such object whenever a table (or view) is used in a query. For
 * example the following query has 2 table filters: SELECT * FROM TEST T1, TEST T2.
 */
public class TableFilter implements ColumnResolver {

    private static final int BEFORE_FIRST = 0, FOUND = 1, AFTER_LAST = 2, NULL_ROW = 3;

    private final Table table;

    private final Select select;

    private String alias;

    private Session session;

    private Index index;

    private IndexColumn[] indexColumns;

    private Cursor cursor;

    private int scanCount;

    /**
     * Indicates that this filter is used in the plan.
     */
    private boolean used;

    // conditions that can be used for direct index lookup (start or end)
    private final ObjectArray indexConditions = new ObjectArray();

    // conditions that can't be used for index lookup,
    // but for row filter for this table (ID=ID, NAME LIKE '%X%')
    private Expression filterCondition;

    // the complete join condition
    private Expression joinCondition;

    private SearchRow currentSearchRow;

    private Row current;

    private int state;

    private TableFilter join;

    private boolean outerJoin;

    private ObjectArray naturalJoinColumns;

    private boolean foundOne;

    private Expression fullCondition;

    /**
     * Create a new table filter object.
     * 
     * @param session
     *            the session
     * @param table
     *            the table from where to read data
     * @param alias
     *            the alias name
     * @param rightsChecked
     *            true if rights are already checked
     * @param select
     *            the select statement
     */
    public TableFilter(final Session session, final Table table, final String alias, final boolean rightsChecked, final Select select) throws SQLException {

        this.session = session;
        this.table = table;
        this.alias = alias;
        this.select = select;

        if (!rightsChecked) {
            session.getUser().checkRight(table, Right.SELECT);
        }
    }

    @Override
    public Select getSelect() {

        return select;
    }

    public Table getTable() {

        return table;
    }

    /**
     * Lock the table. This will also lock joined tables.
     * 
     * @param session
     *            the session
     * @param exclusive
     *            true if an exclusive lock is required
     * @param force
     *            lock even in the MVCC mode
     */
    public void lock(final Session session, final boolean exclusive, final boolean force) throws SQLException {

        table.lock(session, exclusive, force);
        if (join != null) {
            join.lock(session, exclusive, force);
        }
    }

    /**
     * Get the best plan item (index, cost) to use use for the current join order.
     * 
     * @param session
     *            the session
     * @return the best plan item
     */
    public PlanItem getBestPlanItem(final Session session) throws SQLException {

        PlanItem item;
        if (indexConditions.size() == 0) {
            item = new PlanItem();
            item.setIndex(table.getScanIndex(session));
            item.cost = item.getIndex().getCost(session, null);
        }
        else {
            final int len = table.getColumns().length;
            int[] masks = new int[len];
            for (int i = 0; i < indexConditions.size(); i++) {
                final IndexCondition condition = (IndexCondition) indexConditions.get(i);
                if (condition.isEvaluatable()) {
                    if (condition.isAlwaysFalse()) {
                        masks = null;
                        break;
                    }
                    final int id = condition.getColumn().getColumnId();
                    masks[id] |= condition.getMask();
                }
            }
            item = table.getBestPlanItem(session, masks);
        }
        if (join != null) {
            setEvaluatable(join);
            item.setJoinPlan(join.getBestPlanItem(session));
            // TODO optimizer: calculate cost of a join: should use separate
            // expected row number and lookup cost
            item.cost += item.cost * item.getJoinPlan().cost;
        }
        return item;
    }

    private void setEvaluatable(TableFilter join) {

        // this table filter is now evaluatable - in all sub-joins
        do {
            final Expression e = join.getJoinCondition();
            if (e != null) {
                e.setEvaluatable(this, true);
            }
            join = join.getJoin();
        }
        while (join != null);
    }

    /**
     * Set what plan item (index, cost) to use use.
     * 
     * @param item
     *            the plan item
     */
    public void setPlanItem(final PlanItem item) {

        setIndex(item.getIndex());
        if (join != null) {
            if (item.getJoinPlan() != null) {
                join.setPlanItem(item.getJoinPlan());
            }
        }
    }

    /**
     * Prepare reading rows. This method will remove all index conditions that can not be used, and optimize the conditions.
     */
    public void prepare() throws SQLException {

        // forget all unused index conditions
        for (int i = 0; i < indexConditions.size(); i++) {
            final IndexCondition condition = (IndexCondition) indexConditions.get(i);
            if (!condition.isAlwaysFalse()) {
                final Column col = condition.getColumn();
                if (index.getColumnIndex(col) < 0) {
                    indexConditions.remove(i);
                    i--;
                }
            }
        }
        if (join != null) {
            if (SysProperties.CHECK && join == this) {
                Message.throwInternalError("self join");
            }
            join.prepare();
        }
        if (filterCondition != null) {
            filterCondition = filterCondition.optimize(session);
        }
        if (joinCondition != null) {
            joinCondition = joinCondition.optimize(session);
        }
    }

    /**
     * Start the query. This will reset the scan counts.
     * 
     * @param session
     *            the session
     */
    public void startQuery(final Session session) {

        this.session = session;
        scanCount = 0;
        if (join != null) {
            join.startQuery(session);
        }
    }

    /**
     * Reset to the current position.
     */
    public void reset() {

        if (join != null) {
            join.reset();
        }
        state = BEFORE_FIRST;
        foundOne = false;
    }

    private Value getMax(final Value a, final Value b, final boolean bigger) throws SQLException {

        if (a == null) {
            return b;
        }
        else if (b == null) { return a; }
        int comp = a.compareTo(b, session.getDatabase().getCompareMode());
        if (!bigger) {
            comp = -comp;
        }
        return comp > 0 ? a : b;
    }

    /**
     * Check if there are more rows to read.
     * 
     * @return true if there are
     */
    public boolean next() throws SQLException {

        try {
            boolean alwaysFalse = false;
            if (state == AFTER_LAST) {
                return false;
            }
            else if (state == BEFORE_FIRST) {
                SearchRow start = null, end = null;
                for (int i = 0; i < indexConditions.size(); i++) {
                    final IndexCondition condition = (IndexCondition) indexConditions.get(i);
                    if (condition.isAlwaysFalse()) {
                        alwaysFalse = true;
                        break;
                    }
                    final Column column = condition.getColumn();
                    final int type = column.getType();
                    final int id = column.getColumnId();
                    final Value v = condition.getCurrentValue(session).convertTo(type);
                    boolean isStart = condition.isStart(), isEnd = condition.isEnd();
                    final IndexColumn idxCol = indexColumns[id];
                    if (idxCol != null && (idxCol.sortType & SortOrder.DESCENDING) != 0) {
                        // if the index column is sorted the other way, we swap end
                        // and start
                        // NULLS_FIRST / NULLS_LAST is not a problem, as nulls never
                        // match anyway
                        final boolean temp = isStart;
                        isStart = isEnd;
                        isEnd = temp;
                    }
                    if (isStart) {
                        Value newStart;
                        if (start == null) {
                            start = table.getTemplateRow();
                            newStart = v;
                        }
                        else {
                            newStart = getMax(start.getValue(id), v, true);
                        }
                        start.setValue(id, newStart);
                    }
                    if (isEnd) {
                        Value newEnd;
                        if (end == null) {
                            end = table.getTemplateRow();
                            newEnd = v;
                        }
                        else {
                            newEnd = getMax(end.getValue(id), v, false);
                        }
                        end.setValue(id, newEnd);
                    }
                }
                if (!alwaysFalse) {
                    cursor = index.find(session, start, end);
                    if (join != null) {
                        join.reset();
                    }
                }
            }
            else {
                // state == FOUND || LAST_ROW
                // the last row was ok - try next row of the join
                if (join != null && join.next()) { return true; }
            }
            while (true) {
                // go to the next row
                if (state == NULL_ROW) {
                    break;
                }
                if (alwaysFalse) {
                    state = AFTER_LAST;
                }
                else {
                    if ((++scanCount & 4095) == 0) {
                        checkTimeout();
                    }
                    if (cursor.next()) {
                        currentSearchRow = cursor.getSearchRow();
                        current = null;
                        // cursor.get();
                        state = FOUND;
                    }
                    else {
                        state = AFTER_LAST;
                    }
                }
                // if no more rows found, try the null row (for outer joins only)
                if (state == AFTER_LAST) {
                    if (outerJoin && !foundOne) {
                        state = NULL_ROW;
                        current = table.getNullRow();
                        currentSearchRow = current;
                    }
                    else {
                        break;
                    }
                }
                if (!isOk(filterCondition)) {
                    continue;
                }
                final boolean joinConditionOk = isOk(joinCondition);
                if (state == FOUND) {
                    if (joinConditionOk) {
                        foundOne = true;
                    }
                    else {
                        continue;
                    }
                }
                if (join != null) {
                    join.reset();
                    if (!join.next()) {
                        continue;
                    }
                }
                // check if it's ok
                if (state == NULL_ROW || joinConditionOk) { return true; }
            }
            state = AFTER_LAST;
            return false;
        }
        catch (final SQLException e) {

            session.getDatabase().getSchema(session.getCurrentSchemaName()).removeLinkedTable(table, null);
            throw e;
        }
    }

    private void checkTimeout() throws SQLException {

        session.checkCanceled();
        // System.out.println(this.alias+ " " + table.getName() + ": " +
        // scanCount);
    }

    private boolean isOk(final Expression condition) throws SQLException {

        if (condition == null) { return true; }
        return Boolean.TRUE.equals(condition.getBooleanValue(session));
    }

    /**
     * Get the current row.
     * 
     * @return the current row, or null
     */
    public Row get() throws SQLException {

        if (current == null && currentSearchRow != null) {
            if (table.getClustered()) {
                current = table.getTemplateRow();
                for (int i = 0; i < currentSearchRow.getColumnCount(); i++) {
                    current.setValue(i, currentSearchRow.getValue(i));
                }
            }
            else {
                current = cursor.get();
            }
        }
        return current;
    }

    /**
     * Set the current row.
     * 
     * @param current
     *            the current row
     */
    public void set(final Row current) {

        // this is currently only used so that check constraints work - to set
        // the current (new) row
        this.current = current;
        currentSearchRow = current;
    }

    /**
     * Get the table alias name. If no alias is specified, the table name is returned.
     * 
     * @return the alias name
     */
    @Override
    public String getTableAlias() {

        if (alias != null) { return alias; }
        return table.getName();
    }

    /**
     * Add an index condition.
     * 
     * @param condition
     *            the index condition
     */
    public void addIndexCondition(final IndexCondition condition) {

        indexConditions.add(condition);
    }

    /**
     * Add a filter condition.
     * 
     * @param condition
     *            the condition
     * @param join
     *            if this is in fact a join condition
     */
    public void addFilterCondition(final Expression condition, final boolean join) {

        if (join) {
            if (joinCondition == null) {
                joinCondition = condition;
            }
            else {
                joinCondition = new ConditionAndOr(ConditionAndOr.AND, joinCondition, condition);
            }
        }
        else {
            if (filterCondition == null) {
                filterCondition = condition;
            }
            else {
                filterCondition = new ConditionAndOr(ConditionAndOr.AND, filterCondition, condition);
            }
        }
    }

    /**
     * Add a joined table.
     * 
     * @param filter
     *            the joined table filter
     * @param outer
     *            if this is an outer join
     * @param on
     *            the join condition
     */
    public void addJoin(final TableFilter filter, final boolean outer, final Expression on) throws SQLException {

        if (on != null) {
            on.mapColumns(this, 0);
        }
        if (join == null) {
            join = filter;
            filter.outerJoin = outer;
            if (on != null) {
                filter.mapAndAddFilter(on);
            }
        }
        else {
            join.addJoin(filter, outer, on);
        }
    }

    /**
     * Map the columns and add the join condition.
     * 
     * @param on
     *            the condition
     */
    public void mapAndAddFilter(final Expression on) throws SQLException {

        on.mapColumns(this, 0);
        addFilterCondition(on, true);
        on.createIndexConditions(session, this);
        if (join != null) {
            join.mapAndAddFilter(on);
        }
    }

    public TableFilter getJoin() {

        return join;
    }

    /**
     * Check if this is an outer joined table.
     * 
     * @return true if it is
     */
    public boolean isJoinOuter() {

        return outerJoin;
    }

    /**
     * Get the query execution plan text to use for this table filter.
     * 
     * @param join
     *            if this is a joined table
     * @return the SQL statement snippet
     */
    public String getPlanSQL(final boolean join) {

        final StringBuilder buff = new StringBuilder();
        if (join) {
            if (outerJoin) {
                buff.append("LEFT OUTER JOIN ");
            }
            else {
                buff.append("INNER JOIN ");
            }
        }
        buff.append(table.getSQL());
        if (alias != null) {
            buff.append(' ');
            buff.append(Parser.quoteIdentifier(alias));
        }
        if (index != null) {
            buff.append(" /* ");
            final StringBuilder planBuff = new StringBuilder();
            planBuff.append(index.getPlanSQL());
            if (indexConditions.size() > 0) {
                planBuff.append(": ");
                for (int i = 0; i < indexConditions.size(); i++) {
                    final IndexCondition condition = (IndexCondition) indexConditions.get(i);
                    if (i > 0) {
                        planBuff.append(" AND ");
                    }
                    planBuff.append(condition.getSQL());
                }
            }
            String plan = planBuff.toString();
            plan = StringUtils.quoteRemarkSQL(plan);
            buff.append(plan);
            buff.append(" */");
        }
        if (join) {
            buff.append(" ON ");
            if (joinCondition == null) {
                // need to have a ON expression, otherwise the nesting is
                // unclear
                buff.append("1=1");
            }
            else {
                buff.append(StringUtils.unEnclose(joinCondition.getSQL()));
            }
        }
        if (filterCondition != null) {
            buff.append(" /* WHERE ");
            String condition = StringUtils.unEnclose(filterCondition.getSQL());
            condition = StringUtils.quoteRemarkSQL(condition);
            buff.append(condition);
            buff.append(" */");
        }
        return buff.toString();
    }

    /**
     * Remove all index conditions that are not used by the current index.
     */
    void removeUnusableIndexConditions() {

        for (int i = 0; i < indexConditions.size(); i++) {
            final IndexCondition cond = (IndexCondition) indexConditions.get(i);
            if (!cond.isEvaluatable()) {
                indexConditions.remove(i--);
            }
        }
    }

    public Index getIndex() {

        return index;
    }

    public void setIndex(final Index index) {

        this.index = index;
        final Column[] columns = table.getColumns();
        indexColumns = new IndexColumn[columns.length];
        final IndexColumn[] idxCols = index.getIndexColumns();
        if (idxCols != null) {
            for (int i = 0; i < columns.length; i++) {
                final int idx = index.getColumnIndex(columns[i]);
                if (idx >= 0) {
                    indexColumns[i] = idxCols[idx];
                }
            }
        }
    }

    public void setUsed(final boolean used) {

        this.used = used;
    }

    public boolean getUsed() {

        return used;
    }

    /**
     * Set the session of this table filter.
     * 
     * @param session
     *            the new session
     */
    void setSession(final Session session) {

        this.session = session;
    }

    /**
     * Remove the joined table
     */
    public void removeJoin() {

        join = null;
    }

    public Expression getJoinCondition() {

        return joinCondition;
    }

    /**
     * Remove the join condition.
     */
    public void removeJoinCondition() {

        joinCondition = null;
    }

    public Expression getFilterCondition() {

        return filterCondition;
    }

    /**
     * Remove the filter condition.
     */
    public void removeFilterCondition() {

        filterCondition = null;
    }

    public void setFullCondition(final Expression condition) {

        fullCondition = condition;
        if (join != null) {
            join.setFullCondition(condition);
        }
    }

    /**
     * Optimize the full condition. This will add the full condition to the filter condition.
     * 
     * @param fromOuterJoin
     *            if this method was called from an outer joined table
     */
    void optimizeFullCondition(final boolean fromOuterJoin) {

        if (fullCondition != null) {
            fullCondition.addFilterConditions(this, fromOuterJoin || outerJoin);
            if (join != null) {
                join.optimizeFullCondition(fromOuterJoin || outerJoin);
            }
        }
    }

    /**
     * Update the filter and join conditions of this and all joined tables with the information that the given table filter can now return
     * rows or not.
     * 
     * @param filter
     *            the table filter
     * @param b
     *            the new flag
     */
    public void setEvaluatable(final TableFilter filter, final boolean b) {

        if (filterCondition != null) {
            filterCondition.setEvaluatable(filter, b);
        }
        if (joinCondition != null) {
            joinCondition.setEvaluatable(filter, b);
        }
        if (join != null) {
            join.setEvaluatable(filter, b);
        }
    }

    @Override
    public String getSchemaName() {

        return table.getSchema().getName();
    }

    @Override
    public Column[] getColumns() {

        return table.getColumns();
    }

    /**
     * Get the system columns that this table understands. This is used for compatibility with other databases. The columns are only
     * returned if the current mode supports system columns.
     * 
     * @return the system columns
     */
    @Override
    public Column[] getSystemColumns() {

        if (!session.getDatabase().getMode().systemColumns) { return null; }
        final Column[] sys = new Column[3];
        sys[0] = new Column("oid", Value.INT);
        sys[0].setTable(table, 0);
        sys[1] = new Column("ctid", Value.STRING);
        sys[1].setTable(table, 0);
        sys[2] = new Column("CTID", Value.STRING);
        sys[2].setTable(table, 0);
        return sys;
    }

    @Override
    public Value getValue(final Column column) throws SQLException {

        if (currentSearchRow == null) { return null; }
        final int columnId = column.getColumnId();
        if (current == null) {
            final Value v = currentSearchRow.getValue(columnId);
            if (v != null) { return v; }
            current = cursor.get();
        }
        return current.getValue(columnId);
    }

    @Override
    public TableFilter getTableFilter() {

        return this;
    }

    public void setAlias(final String alias) {

        this.alias = alias;
    }

    @Override
    public Expression optimize(final ExpressionColumn expressionColumn, final Column column) {

        return expressionColumn;
    }

    @Override
    public String toString() {

        return alias != null ? alias : "" + table;
    }

    /**
     * Add a column to the natural join key column list.
     * 
     * @param c
     *            the column to add
     */
    public void addNaturalJoinColumn(final Column c) {

        if (naturalJoinColumns == null) {
            naturalJoinColumns = new ObjectArray();
        }
        naturalJoinColumns.add(c);
    }

    /**
     * Check if the given column is a natural join column.
     * 
     * @param c
     *            the column to check
     * @return true if this is a joined natural join column
     */
    public boolean isNaturalJoinColumn(final Column c) {

        return naturalJoinColumns != null && naturalJoinColumns.indexOf(c) >= 0;
    }

}
