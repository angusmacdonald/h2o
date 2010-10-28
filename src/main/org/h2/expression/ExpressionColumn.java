/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.expression;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.command.Parser;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectListColumnResolver;
import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexCondition;
import org.h2.message.Message;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;

/**
 * A expression that represents a column of a table or view.
 */
public class ExpressionColumn extends Expression {

    private final Database database;

    private String schemaName;

    private String tableAlias;

    private String columnName;

    private ColumnResolver resolver;

    private int queryLevel;

    private Column column;

    private boolean evaluatable;

    public ExpressionColumn(final Database database, final Column column) {

        this.database = database;
        this.column = column;
    }

    public ExpressionColumn(final Database database, final String schemaName, final String tableAlias, final String columnName) {

        this.database = database;
        this.schemaName = schemaName;
        this.tableAlias = tableAlias;
        this.columnName = columnName;
    }

    @Override
    public String getSQL() {

        String sql;
        if (column != null) {
            sql = column.getSQL();
        }
        else {
            sql = Parser.quoteIdentifier(columnName);
        }
        if (tableAlias != null) {
            sql = Parser.quoteIdentifier(tableAlias) + "." + sql;
        }
        if (schemaName != null) {
            sql = Parser.quoteIdentifier(schemaName) + "." + sql;
        }
        return sql;
    }

    public TableFilter getTableFilter() {

        return resolver == null ? null : resolver.getTableFilter();
    }

    @Override
    public void mapColumns(final ColumnResolver resolver, final int level) throws SQLException {

        if (tableAlias != null && !tableAlias.equals(resolver.getTableAlias())) { return; }
        if (schemaName != null && !schemaName.equals(resolver.getSchemaName())) { return; }
        Column[] columns = resolver.getColumns();
        for (final Column col : columns) {
            if (columnName.equals(col.getName())) {
                mapColumn(resolver, col, level);
                return;
            }
        }
        columns = resolver.getSystemColumns();
        for (int i = 0; columns != null && i < columns.length; i++) {
            final Column col = columns[i];
            if (columnName.equals(col.getName())) {
                mapColumn(resolver, col, level);
                return;
            }
        }
    }

    private void mapColumn(final ColumnResolver resolver, final Column col, final int level) throws SQLException {

        if (this.resolver == null) {
            queryLevel = level;
            column = col;
            this.resolver = resolver;
        }
        else if (queryLevel == level && this.resolver != resolver) {
            if (resolver instanceof SelectListColumnResolver) {
                // ignore - already mapped, that's ok
            }
            else {
                throw Message.getSQLException(ErrorCode.AMBIGUOUS_COLUMN_NAME_1, columnName);
            }
        }
    }

    @Override
    public Expression optimize(final Session session) throws SQLException {

        if (resolver == null) {
            final Schema schema = session.getDatabase().findSchema(tableAlias == null ? session.getCurrentSchemaName() : tableAlias);
            if (schema != null) {
                final Constant constant = schema.findConstant(columnName);
                if (constant != null) { return constant.getValue(); }
            }
            String name = columnName;
            if (tableAlias != null) {
                name = tableAlias + "." + name;
                if (schemaName != null) {
                    name = schemaName + "." + name;
                }
            }
            throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, name);
        }
        return resolver.optimize(this, column);
    }

    @Override
    public void updateAggregate(final Session session) throws SQLException {

        final Value now = resolver.getValue(column);
        final Select select = resolver.getSelect();
        if (select == null) { throw Message.getSQLException(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL()); }
        final HashMap values = select.getCurrentGroup();
        if (values == null) {
            // this is a different level (the enclosing query)
            return;
        }
        final Value v = (Value) values.get(this);
        if (v == null) {
            values.put(this, now);
        }
        else {
            if (!database.areEqual(now, v)) { throw Message.getSQLException(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL()); }
        }
    }

    @Override
    public Value getValue(final Session session) throws SQLException {

        // TODO refactor: simplify check if really part of an aggregated value /
        // detection of
        // usage of non-grouped by columns without aggregate function
        final Select select = resolver.getSelect();
        if (select != null) {
            final HashMap values = select.getCurrentGroup();
            if (values != null) {
                final Value v = (Value) values.get(this);
                if (v != null) { return v; }
            }
        }
        final Value value = resolver.getValue(column);
        if (value == null) {
            final int i = 0;
            throw Message.getSQLException(ErrorCode.MUST_GROUP_BY_COLUMN_1, getSQL());
        }
        return value;
    }

    @Override
    public int getType() {

        return column.getType();
    }

    @Override
    public void setEvaluatable(final TableFilter tableFilter, final boolean b) {

        if (resolver != null && tableFilter == resolver.getTableFilter()) {
            evaluatable = b;
        }
    }

    public Column getColumn() {

        return column;
    }

    @Override
    public int getScale() {

        return column.getScale();
    }

    @Override
    public long getPrecision() {

        return column.getPrecision();
    }

    @Override
    public int getDisplaySize() {

        return column.getDisplaySize();
    }

    public String getOriginalColumnName() {

        return columnName;
    }

    public String getOriginalTableAliasName() {

        return tableAlias;
    }

    @Override
    public String getColumnName() {

        return columnName != null ? columnName : column.getName();
    }

    @Override
    public String getSchemaName() {

        final Table table = column.getTable();
        return table == null ? null : table.getSchema().getName();
    }

    @Override
    public String getTableName() {

        final Table table = column.getTable();
        return table == null ? null : table.getName();
    }

    @Override
    public String getAlias() {

        return column == null ? null : column.getName();
    }

    @Override
    public boolean isAutoIncrement() {

        return column.getSequence() != null;
    }

    @Override
    public int getNullable() {

        return column.getNullable() ? Column.NULLABLE : Column.NOT_NULLABLE;
    }

    @Override
    public boolean isEverything(final ExpressionVisitor visitor) {

        switch (visitor.getType()) {
            case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
                return false;
            case ExpressionVisitor.READONLY:
            case ExpressionVisitor.DETERMINISTIC:
                return true;
            case ExpressionVisitor.INDEPENDENT:
                return queryLevel < visitor.getQueryLevel();
            case ExpressionVisitor.EVALUATABLE:
                // if the current value is known (evaluatable set)
                // or if this columns belongs to a 'higher level' query and is
                // therefore just a parameter
                return evaluatable || visitor.getQueryLevel() < queryLevel;
            case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
                visitor.addDataModificationId(column.getTable().getMaxDataModificationId());
                return true;
            case ExpressionVisitor.NOT_FROM_RESOLVER:
                return resolver != visitor.getResolver();
            case ExpressionVisitor.GET_DEPENDENCIES:
                visitor.addDependency(column.getTable());
                return true;
            default:
                throw Message.throwInternalError("type=" + visitor.getType());
        }
    }

    @Override
    public int getCost() {

        return 2;
    }

    @Override
    public void createIndexConditions(final Session session, final TableFilter filter) {

        final TableFilter tf = getTableFilter();
        if (filter == tf && column.getType() == Value.BOOLEAN) {
            final IndexCondition cond = new IndexCondition(Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(true)));
            filter.addIndexCondition(cond);
        }
    }

    @Override
    public Expression getNotIfPossible(final Session session) {

        return new Comparison(session, Comparison.EQUAL, this, ValueExpression.get(ValueBoolean.get(false)));
    }

}
