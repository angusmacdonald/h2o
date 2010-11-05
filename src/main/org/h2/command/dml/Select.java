/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.expression.Alias;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.Wildcard;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents a simple SELECT statement.
 * 
 * For each select statement, visibleColumnCount &lt;= distinctColumnCount &lt;= expressionCount. The expression list count could include
 * ORDER BY and GROUP BY expressions that are not in the select list.
 * 
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 * 
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {

    private TableFilter topTableFilter;

    private final ObjectArray filters = new ObjectArray();

    private final ObjectArray topFilters = new ObjectArray();

    private ObjectArray expressions;

    private Expression having;

    private Expression condition;

    private int visibleColumnCount, distinctColumnCount;

    private ObjectArray orderList;

    private ObjectArray group;

    private int[] groupIndex;

    private boolean[] groupByExpression;

    private boolean distinct;

    private HashMap currentGroup;

    private int havingIndex;

    private boolean isGroupQuery, isGroupSortedQuery;

    private boolean isForUpdate;

    private double cost;

    private boolean isQuickAggregateQuery, isDistinctQuery;

    private boolean isPrepared, checkInit;

    private boolean sortUsingIndex;

    private SortOrder sort;

    private int currentGroupRowId;

    private LocationPreference locationPreference = LocationPreference.NO_PREFERENCE;

    public Select(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    /**
     * Add a table to the query.
     * 
     * @param filter
     *            the table to add
     * @param isTop
     *            if the table can be the first table in the query plan
     */
    public void addTableFilter(final TableFilter filter, final boolean isTop) {

        // TODO compatibility: it seems oracle doesn't check on
        // duplicate aliases; do other databases check it?
        // String alias = filter.getAlias();
        // if(filterNames.contains(alias)) {
        // throw Message.getSQLException(
        // ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ObjectArray getTopFilters() {

        return topFilters;
    }

    public void setExpressions(final ObjectArray expressions) {

        this.expressions = expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {

        isGroupQuery = true;
    }

    public void setGroupBy(final ObjectArray group) {

        this.group = group;
    }

    public HashMap getCurrentGroup() {

        return currentGroup;
    }

    public int getCurrentGroupRowId() {

        return currentGroupRowId;
    }

    @Override
    public void setOrder(final ObjectArray order) {

        orderList = order;
    }

    /**
     * Add a condition to the list of conditions.
     * 
     * @param cond
     *            the condition to add
     */
    public void addCondition(final Expression cond) {

        if (condition == null) {
            condition = cond;
        }
        else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    private void queryGroupSorted(final int columnCount, final LocalResult result) throws SQLException {

        int rowNumber = 0;
        setCurrentRowNumber(0);
        Value[] previousKeyValues = null;
        while (topTableFilter.next()) {
            checkCanceled();
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                rowNumber++;
                final Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    final int idx = groupIndex[i];
                    final Expression expr = (Expression) expressions.get(idx);
                    keyValues[i] = expr.getValue(session);
                }

                if (previousKeyValues == null) {
                    previousKeyValues = keyValues;
                    currentGroup = new HashMap();
                }
                else if (!Arrays.equals(previousKeyValues, keyValues)) {
                    addGroupSortedRow(previousKeyValues, columnCount, result);
                    previousKeyValues = keyValues;
                    currentGroup = new HashMap();
                }
                currentGroupRowId++;

                for (int i = 0; i < columnCount; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        final Expression expr = (Expression) expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
            }
        }
        if (previousKeyValues != null) {
            addGroupSortedRow(previousKeyValues, columnCount, result);
        }
    }

    private void addGroupSortedRow(final Value[] keyValues, final int columnCount, final LocalResult result) throws SQLException {

        Value[] row = new Value[columnCount];
        for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
            row[groupIndex[j]] = keyValues[j];
        }
        for (int j = 0; j < columnCount; j++) {
            if (groupByExpression != null && groupByExpression[j]) {
                continue;
            }
            final Expression expr = (Expression) expressions.get(j);
            row[j] = expr.getValue(session);
        }
        if (isHavingNullOrFalse(row)) { return; }
        row = keepOnlyDistinct(row, columnCount);
        result.addRow(row);
    }

    private Value[] keepOnlyDistinct(final Value[] row, final int columnCount) {

        if (columnCount == distinctColumnCount) { return row; }
        // remove columns so that 'distinct' can filter duplicate rows
        final Value[] r2 = new Value[distinctColumnCount];
        ObjectUtils.arrayCopy(row, r2, distinctColumnCount);
        return r2;
    }

    private boolean isHavingNullOrFalse(final Value[] row) throws SQLException {

        if (havingIndex >= 0) {
            final Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) { return true; }
            if (!Boolean.TRUE.equals(v.getBoolean())) { return true; }
        }
        return false;
    }

    private Index getGroupSortedIndex() {

        if (groupIndex == null || groupByExpression == null) { return null; }
        final ObjectArray indexes = topTableFilter.getTable().getIndexes();
        for (int i = 0; indexes != null && i < indexes.size(); i++) {
            final Index index = (Index) indexes.get(i);
            if (index.getIndexType().getScan()) {
                continue;
            }
            if (isGroupSortedIndex(index)) { return index; }
        }
        return null;
    }

    private boolean isGroupSortedIndex(final Index index) {

        // check that all the GROUP BY expressions are part of the index
        final Column[] indexColumns = index.getColumns();
        // also check that the first columns in the index are grouped
        final boolean[] grouped = new boolean[indexColumns.length];
        outerLoop: for (int i = 0; i < expressions.size(); i++) {
            if (!groupByExpression[i]) {
                continue;
            }
            final Expression expr = (Expression) expressions.get(i);
            if (!(expr instanceof ExpressionColumn)) { return false; }
            final ExpressionColumn exprCol = (ExpressionColumn) expr;
            for (int j = 0; j < indexColumns.length; ++j) {
                if (indexColumns[j].equals(exprCol.getColumn())) {
                    grouped[j] = true;
                    continue outerLoop;
                }
            }
            // We didn't find a matching index column
            // for one group by expression
            return false;
        }
        // check that the first columns in the index are grouped
        // good: index(a, b, c); group by b, a
        // bad: index(a, b, c); group by a, c
        for (int i = 1; i < grouped.length; i++) {
            if (!grouped[i - 1] && grouped[i]) { return false; }
        }
        return true;
    }

    private int getGroupByExpressionCount() {

        if (groupByExpression == null) { return 0; }
        int count = 0;
        for (final boolean element : groupByExpression) {
            if (element) {
                ++count;
            }
        }
        return count;
    }

    private void queryGroup(final int columnCount, final LocalResult result) throws SQLException {

        final ValueHashMap groups = new ValueHashMap(session.getDatabase());
        int rowNumber = 0;
        setCurrentRowNumber(0);
        final ValueArray defaultGroup = ValueArray.get(new Value[0]);
        while (topTableFilter.next()) {
            checkCanceled();
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                }
                else {
                    final Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        final int idx = groupIndex[i];
                        final Expression expr = (Expression) expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap values = (HashMap) groups.get(key);
                if (values == null) {
                    values = new HashMap();
                    groups.put(key, values);
                }
                currentGroup = values;
                currentGroupRowId++;
                final int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        final Expression expr = (Expression) expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap());
        }
        final ObjectArray keys = groups.keys();
        for (int i = 0; i < keys.size(); i++) {
            final ValueArray key = (ValueArray) keys.get(i);
            currentGroup = (HashMap) groups.get(key);
            final Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                final Expression expr = (Expression) expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }

    /**
     * Get the index that matches the ORDER BY list, if one exists. This is to avoid running a separate ORDER BY if an index can be used.
     * This is specially important for large result sets, if only the first few rows are important (LIMIT is used)
     * 
     * @return the index if one is found
     */
    private Index getSortIndex() throws SQLException {

        if (sort == null) { return null; }
        final int[] indexes = sort.getIndexes();
        final ObjectArray sortColumns = new ObjectArray();
        for (final int idx : indexes) {
            if (idx < 0 || idx >= expressions.size()) { throw Message.getInvalidValueException("" + (idx + 1), "ORDER BY"); }
            Expression expr = (Expression) expressions.get(idx);
            expr = expr.getNonAliasExpression();
            if (expr.isConstant()) {
                continue;
            }
            if (!(expr instanceof ExpressionColumn)) { return null; }
            final ExpressionColumn exprCol = (ExpressionColumn) expr;
            if (exprCol.getTableFilter() != topTableFilter) { return null; }
            sortColumns.add(exprCol.getColumn());
        }
        final Column[] sortCols = new Column[sortColumns.size()];
        sortColumns.toArray(sortCols);
        final int[] sortTypes = sort.getSortTypes();
        if (sortCols.length == 0) {
            // sort just on constants - can use scan index
            return topTableFilter.getTable().getScanIndex(session);
        }
        final ObjectArray list = topTableFilter.getTable().getIndexes();
        for (int i = 0; list != null && i < list.size(); i++) {
            final Index index = (Index) list.get(i);
            if (index.getCreateSQL() == null) {
                // can't use the scan index
                continue;
            }
            if (index.getIndexType().getHash()) {
                continue;
            }
            final IndexColumn[] indexCols = index.getIndexColumns();
            if (indexCols.length < sortCols.length) {
                continue;
            }
            boolean ok = true;
            for (int j = 0; j < sortCols.length; j++) {
                // the index and the sort order must start
                // with the exact same columns
                final IndexColumn idxCol = indexCols[j];
                final Column sortCol = sortCols[j];
                if (idxCol.column != sortCol) {
                    ok = false;
                    break;
                }
                if (idxCol.sortType != sortTypes[j]) {
                    // TODO NULL FIRST for ascending and NULLS LAST
                    // for descending would actually match the default
                    ok = false;
                    break;
                }
            }
            if (ok) { return index; }
        }
        return null;
    }

    private void queryDistinct(final LocalResult result, long limitRows) throws SQLException {

        if (limitRows != 0 && offset != null) {
            // limitRows must be long, otherwise we get an int overflow
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offset.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        final Index index = topTableFilter.getIndex();
        SearchRow first = null;
        final int columnIndex = index.getColumns()[0].getColumnId();
        while (true) {
            checkCanceled();
            setCurrentRowNumber(rowNumber + 1);
            final Cursor cursor = index.findNext(session, first, null);
            if (!cursor.next()) {
                break;
            }
            final SearchRow found = cursor.getSearchRow();
            final Value value = found.getValue(columnIndex);
            if (first == null) {
                first = topTableFilter.getTable().getTemplateSimpleRow(true);
            }
            first.setValue(columnIndex, value);
            final Value[] row = new Value[1];
            row[0] = value;
            result.addRow(row);
            rowNumber++;
            if ((sort == null || sortUsingIndex) && limitRows != 0 && result.getRowCount() >= limitRows) {
                break;
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }
        }
    }

    private void queryFlat(final int columnCount, final LocalResult result, long limitRows) throws SQLException {

        if (limitRows != 0 && offset != null) {
            // limitRows must be long, otherwise we get an int overflow
            // if limitRows is at or near Integer.MAX_VALUE
            limitRows += offset.getValue(session).getInt();
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        while (topTableFilter.next()) {
            checkCanceled();
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                final Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    final Expression expr = (Expression) expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                result.addRow(row);
                rowNumber++;
                if ((sort == null || sortUsingIndex) && limitRows != 0 && result.getRowCount() >= limitRows) {
                    break;
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
    }

    private void queryQuick(final int columnCount, final LocalResult result) throws SQLException {

        final Value[] row = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            final Expression expr = (Expression) expressions.get(i);
            row[i] = expr.getValue(session);
        }
        result.addRow(row);
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        final LocalResult result = new LocalResult(session, expressions, visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    protected LocalResult queryWithoutCache(final int maxRows) throws SQLException {

        int limitRows = maxRows;
        if (limit != null) {
            final int l = limit.getValue(session).getInt();
            if (limitRows == 0) {
                limitRows = l;
            }
            else {
                limitRows = Math.min(l, limitRows);
            }
        }
        final int columnCount = expressions.size();
        final LocalResult result = new LocalResult(session, expressions, visibleColumnCount);
        if (!sortUsingIndex) {
            result.setSortOrder(sort);
        }
        if (distinct && !isDistinctQuery) {
            result.setDistinct();
        }
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        topTableFilter.lock(session, isForUpdate, isForUpdate);
        if (isQuickAggregateQuery) {
            queryQuick(columnCount, result);
        }
        else if (isGroupQuery) {
            if (isGroupSortedQuery) {
                queryGroupSorted(columnCount, result);
            }
            else {
                queryGroup(columnCount, result);
            }
        }
        else if (isDistinctQuery) {
            queryDistinct(result, limitRows);
        }
        else {
            queryFlat(columnCount, result, limitRows);
        }
        if (offset != null) {
            result.setOffset(offset.getValue(session).getInt());
        }
        if (limitRows != 0) {
            result.setLimit(limitRows);
        }
        result.done();
        return result;
    }

    private void expandColumnList() throws SQLException {

        // TODO this works: select distinct count(*)
        // from system_columns group by table
        for (int i = 0; i < expressions.size(); i++) {
            final Expression expr = (Expression) expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            final String schemaName = expr.getSchemaName();
            final String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                final int temp = i;
                expressions.remove(i);
                for (int j = 0; j < filters.size(); j++) {
                    final TableFilter filter = (TableFilter) filters.get(j);
                    final Wildcard c2 = new Wildcard(filter.getTable().getSchema().getName(), filter.getTableAlias());
                    expressions.add(i++, c2);
                }
                i = temp - 1;
            }
            else {
                TableFilter filter = null;
                for (int j = 0; j < filters.size(); j++) {
                    final TableFilter f = (TableFilter) filters.get(j);
                    if (tableAlias.equals(f.getTableAlias())) {
                        if (schemaName == null || schemaName.equals(f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias); }
                final Table t = filter.getTable();
                final String alias = filter.getTableAlias();
                expressions.remove(i);
                final Column[] columns = t.getColumns();
                for (final Column c : columns) {
                    if (filter.isNaturalJoinColumn(c)) {
                        continue;
                    }
                    final ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), null, alias, c.getName());
                    expressions.add(i++, ec);
                }
                i--;
            }
        }
    }

    @Override
    public void init() throws SQLException {

        if (SysProperties.CHECK && checkInit) {
            Message.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ObjectArray expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = new ObjectArray();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = (Expression) expressions.get(i);
                expr = expr.getNonAliasExpression();
                final String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        }
        else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(expressions, expressionSQL, orderList, visibleColumnCount, distinct);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        }
        else {
            havingIndex = -1;
        }

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            groupIndex = new int[group.size()];
            for (int i = 0; i < group.size(); i++) {
                final Expression expr = (Expression) group.get(i);
                final String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expressionSQL.size(); j++) {
                    final String s2 = (String) expressionSQL.get(j);
                    if (s2.equals(sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expressionSQL.size(); j++) {
                        final Expression e = (Expression) expressions.get(j);
                        if (sql.equals(e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    final int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                }
                else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int i = 0; i < groupIndex.length; i++) {
                groupByExpression[groupIndex[i]] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (int i = 0; i < filters.size(); i++) {
            final TableFilter f = (TableFilter) filters.get(i);
            for (int j = 0; j < expressions.size(); j++) {
                final Expression expr = (Expression) expressions.get(j);
                expr.mapColumns(f, 0);
            }
            if (condition != null) {
                condition.mapColumns(f, 0);
            }
        }
        if (havingIndex >= 0) {
            final Expression expr = (Expression) expressions.get(havingIndex);
            final SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0);
        }
        checkInit = true;
    }

    @Override
    public void prepare() throws SQLException {

        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            Message.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            final Expression e = (Expression) expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            if (SysProperties.optimizeInJoin) {
                condition = condition.optimizeInJoin(session, this);
            }
            for (int j = 0; j < filters.size(); j++) {
                final TableFilter f = (TableFilter) filters.get(j);
                condition.createIndexConditions(session, f);
            }
        }
        if (isGroupQuery && groupIndex == null && havingIndex < 0 && filters.size() == 1) {
            if (condition == null) {
                final ExpressionVisitor optimizable = ExpressionVisitor.get(ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL);
                optimizable.setTable(((TableFilter) filters.get(0)).getTable());
                isQuickAggregateQuery = isEverything(optimizable);
            }
        }
        cost = preparePlan();
        if (SysProperties.OPTIMIZE_DISTINCT && distinct && !isGroupQuery && filters.size() == 1 && expressions.size() == 1 && condition == null) {
            Expression expr = (Expression) expressions.get(0);
            expr = expr.getNonAliasExpression();
            if (expr instanceof ExpressionColumn) {
                final Column column = ((ExpressionColumn) expr).getColumn();
                final int selectivity = column.getSelectivity();
                final Index columnIndex = topTableFilter.getTable().getIndexForColumn(column, true);
                if (columnIndex != null && selectivity != Constants.SELECTIVITY_DEFAULT && selectivity < 20) {
                    // the first column must be ascending
                    final boolean ascending = columnIndex.getIndexColumns()[0].sortType == SortOrder.ASCENDING;
                    final Index current = topTableFilter.getIndex();
                    // if another index is faster
                    if (columnIndex.canFindNext() && ascending && (current == null || current.getIndexType().getScan() || columnIndex == current)) {
                        final IndexType type = columnIndex.getIndexType();
                        // hash indexes don't work, and unique single column
                        // indexes don't work
                        if (!type.getHash() && (!type.getUnique() || columnIndex.getColumns().length > 1)) {
                            topTableFilter.setIndex(columnIndex);
                            isDistinctQuery = true;
                        }
                    }
                }
            }
        }
        if (sort != null && !isQuickAggregateQuery && !isGroupQuery) {
            final Index index = getSortIndex();
            final Index current = topTableFilter.getIndex();
            if (index != null && (current.getIndexType().getScan() || current == index)) {
                topTableFilter.setIndex(index);
                if (!distinct || isDistinctQuery) {
                    // sort using index would not work correctly for distinct
                    // result sets
                    // because it would break too early when limit is used
                    sortUsingIndex = true;
                }
            }
        }
        if (SysProperties.OPTIMIZE_GROUP_SORTED && !isQuickAggregateQuery && isGroupQuery && getGroupByExpressionCount() > 0) {
            final Index index = getGroupSortedIndex();
            final Index current = topTableFilter.getIndex();
            if (index != null && (current.getIndexType().getScan() || current == index)) {
                topTableFilter.setIndex(index);
                isGroupSortedQuery = true;
            }
        }
        isPrepared = true;
    }

    @Override
    public double getCost() {

        return cost;
    }

    @Override
    public HashSet<Table> getTables() {

        final HashSet<Table> set = new HashSet<Table>();
        for (int i = 0; i < filters.size(); i++) {
            final TableFilter filter = (TableFilter) filters.get(i);
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() throws SQLException {

        final TableFilter[] topArray = new TableFilter[topFilters.size()];
        topFilters.toArray(topArray);
        for (final TableFilter element : topArray) {
            element.setFullCondition(condition);
        }

        final Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        final double cost = optimizer.getCost();

        TableFilter f = topTableFilter;
        while (f != null) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    if (f.isJoinOuter()) {
                        // this will check if all columns exist - it may or may
                        // not throw an exception
                        on = on.optimize(session);
                        // it is not supported even if the columns exist
                        throw Message.getSQLException(ErrorCode.UNSUPPORTED_OUTER_JOIN_CONDITION_1, on.getSQL());
                    }
                    f.removeJoinCondition();
                    // need to check that all added are bound to a table
                    on = on.optimize(session);
                    addCondition(on);
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (int i = 0; i < expressions.size(); i++) {
                final Expression e = (Expression) expressions.get(i);
                e.setEvaluatable(f, true);
            }
            f = f.getJoin();
        }
        topTableFilter.prepare();
        return cost;
    }

    @Override
    public String getPlanSQL() {

        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        final StringBuilder buff = new StringBuilder();
        final Expression[] exprList = new Expression[expressions.size()];
        expressions.toArray(exprList);
        buff.append("SELECT ");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            final Expression expr = exprList[i];
            buff.append(expr.getSQL());
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            int i = 0;
            do {
                if (i > 0) {
                    buff.append("\n");
                }
                buff.append(filter.getPlanSQL(i > 0));
                i++;
                filter = filter.getJoin();
            }
            while (filter != null);
        }
        else {
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    buff.append("\n");
                }
                filter = (TableFilter) filters.get(i);
                buff.append(filter.getPlanSQL(i > 0));
            }
        }
        if (condition != null) {
            buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            for (int i = 0; i < groupIndex.length; i++) {
                Expression g = exprList[groupIndex[i]];
                g = g.getNonAliasExpression();
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            for (int i = 0; i < group.size(); i++) {
                final Expression g = (Expression) group.get(i);
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            final Expression h = having;
            buff.append("\nHAVING " + StringUtils.unEnclose(h.getSQL()));
        }
        else if (havingIndex >= 0) {
            final Expression h = exprList[havingIndex];
            buff.append("\nHAVING " + StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ");
            buff.append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (orderList != null) {
            buff.append("\nORDER BY ");
            for (int i = 0; i < orderList.size(); i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                final SelectOrderBy o = (SelectOrderBy) orderList.get(i);
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
        }
        if (limit != null) {
            buff.append("\nLIMIT ");
            buff.append(StringUtils.unEnclose(limit.getSQL()));
            if (offset != null) {
                buff.append(" OFFSET ");
                buff.append(StringUtils.unEnclose(offset.getSQL()));
            }
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        if (isQuickAggregateQuery) {
            buff.append("\n/* direct lookup */");
        }
        if (isDistinctQuery) {
            buff.append("\n/* distinct */");
        }
        if (sortUsingIndex) {
            buff.append("\n/* index sorted */");
        }
        if (isGroupQuery) {
            if (isGroupSortedQuery) {
                buff.append("\n/* group sorted */");
            }
        }
        return buff.toString();
    }

    @Override
    public void setDistinct(final boolean b) {

        distinct = b;
    }

    public void setHaving(final Expression having) {

        this.having = having;
    }

    @Override
    public int getColumnCount() {

        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {

        return topTableFilter;
    }

    @Override
    public ObjectArray getExpressions() {

        return expressions;
    }

    @Override
    public void setForUpdate(final boolean b) {

        isForUpdate = b;
    }

    @Override
    public void mapColumns(final ColumnResolver resolver, final int level) throws SQLException {

        for (int i = 0; i < expressions.size(); i++) {
            final Expression e = (Expression) expressions.get(i);
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(final TableFilter tableFilter, final boolean b) {

        for (int i = 0; i < expressions.size(); i++) {
            final Expression e = (Expression) expressions.get(i);
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    /**
     * Check if this is an aggregate query with direct lookup, for example a query of the type SELECT COUNT(*) FROM TEST or SELECT MAX(ID)
     * FROM TEST.
     * 
     * @return true if a direct lookup is possible
     */
    public boolean isQuickAggregateQuery() {

        return isQuickAggregateQuery;
    }

    @Override
    public void addGlobalCondition(final Parameter param, final int columnId, final int comparisonType) throws SQLException {

        addParameter(param);
        Expression col = (Expression) expressions.get(columnId);
        col = col.getNonAliasExpression();
        Expression comp = new Comparison(session, comparisonType, col, param);
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = (Expression) expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                }
                else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            }
            else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    @Override
    public void updateAggregate(final Session session) throws SQLException {

        for (int i = 0; i < expressions.size(); i++) {
            final Expression e = (Expression) expressions.get(i);
            e.updateAggregate(session);
        }
        if (condition != null) {
            condition.updateAggregate(session);
        }
        if (having != null) {
            having.updateAggregate(session);
        }
    }

    @Override
    public boolean isEverything(final ExpressionVisitor visitor) {

        switch (visitor.getType()) {
            case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID: {
                for (int i = 0; i < filters.size(); i++) {
                    final TableFilter f = (TableFilter) filters.get(i);
                    final long m = f.getTable().getMaxDataModificationId();
                    visitor.addDataModificationId(m);
                }
                break;
            }
            case ExpressionVisitor.EVALUATABLE: {
                if (!SysProperties.OPTIMIZE_EVALUATABLE_SUBQUERIES) { return false; }
                break;
            }
            case ExpressionVisitor.GET_DEPENDENCIES: {
                for (int i = 0; i < filters.size(); i++) {
                    final TableFilter filter = (TableFilter) filters.get(i);
                    final Table table = filter.getTable();
                    visitor.addDependency(table);
                    table.addDependencies(visitor.getDependencies());
                }
                break;
            }
            default:
        }
        visitor.incrementQueryLevel(1);
        boolean result = true;
        for (int i = 0; i < expressions.size(); i++) {
            final Expression e = (Expression) expressions.get(i);
            if (!e.isEverything(visitor)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(visitor)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(visitor)) {
            result = false;
        }
        visitor.incrementQueryLevel(-1);
        return result;
    }

    @Override
    public boolean isReadOnly() {

        return isEverything(ExpressionVisitor.READONLY);
    }

    @Override
    public String getFirstColumnAlias(final Session session) {

        if (SysProperties.CHECK) {
            if (visibleColumnCount > 1) {
                Message.throwInternalError("" + visibleColumnCount);
            }
        }
        Expression expr = (Expression) expressions.get(0);
        if (expr instanceof Alias) { return expr.getAlias(); }
        final Mode mode = session.getDatabase().getMode();
        final String name = session.getNextSystemIdentifier(getSQL());
        expr = new Alias(expr, name, mode.aliasColumnName);
        expressions.set(0, expr);
        return expr.getAlias();
    }

    /**
     * Specifies whether the user wishes the query to be evaluated locally (LOCAL), at the primary copy (PRIMARY), or if they have no
     * preference (NO_PREFERENCE).
     * 
     * @param b
     *            true if local; false for remote evaluation (i.e. access remote copy of the data).
     */
    public void setLocationPreference(final LocationPreference locale) {

        locationPreference = locale;
    }

    /**
     * Whether the user wishes the query to be evaluated locally (LOCAL), at the primary copy (PRIMARY), or if they have no preference
     * (NO_PREFERENCE).
     */
    public LocationPreference getLocationPreference() {

        return locationPreference;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#acquireLocks(org.h2.h2o.comms.QueryProxyManager)
     */
    @Override
    public void acquireLocks(final QueryProxyManager queryProxyManager) throws SQLException {

        for (final Table table : getTables()) {
            if (!session.getDatabase().isTableLocal(table.getSchema())) {

                if (Table.TABLE.equals(table.getTableType())) {
                    QueryProxy qp = queryProxyManager.getQueryProxy(table.getFullName());

                    if (qp == null || qp.getLockGranted().equals(LockType.NONE)) {
                        qp = QueryProxy.getQueryProxyAndLock(table, LockType.READ, LockRequest.createNewLockRequest(session), session.getDatabase());
                    }

                    queryProxyManager.addProxy(qp);
                }
                else if (Table.VIEW.equals(table.getTableType())) {
                    // Get locks for the tables involved in executing the view.

                    final List<Table> tables = ((TableView) table).getTables();

                    for (final Table theseTables : tables) {
                        if (!session.getDatabase().isTableLocal(theseTables.getSchema())) {

                            final QueryProxy qp = QueryProxy.getQueryProxyAndLock(theseTables, LockType.READ, LockRequest.createNewLockRequest(session), session.getDatabase());
                            queryProxyManager.addProxy(qp);
                        }
                    }
                }
            }
        }
    }

}
