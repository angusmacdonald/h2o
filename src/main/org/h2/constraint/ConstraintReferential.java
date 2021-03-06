/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.constraint;

import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueNull;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * A referential constraint.
 */
public class ConstraintReferential extends Constraint {

    /**
     * The action is to restrict the operation.
     */
    public static final int RESTRICT = 0;

    /**
     * The action is to cascade the operation.
     */
    public static final int CASCADE = 1;

    /**
     * The action is to set the value to the default value.
     */
    public static final int SET_DEFAULT = 2;

    /**
     * The action is to set the value to NULL.
     */
    public static final int SET_NULL = 3;

    private IndexColumn[] columns;

    private IndexColumn[] refColumns;

    private int deleteAction;

    private int updateAction;

    private Table refTable;

    private Index index;

    private Index refIndex;

    private boolean indexOwner;

    private boolean refIndexOwner;

    private String deleteSQL, updateSQL;

    private boolean skipOwnTable;

    public ConstraintReferential(final Schema schema, final int id, final String name, final Table table) {

        super(schema, id, name, table);
    }

    @Override
    public String getConstraintType() {

        return Constraint.REFERENTIAL;
    }

    private void appendAction(final StringBuilder buff, final int action) {

        switch (action) {
            case CASCADE:
                buff.append("CASCADE");
                break;
            case SET_DEFAULT:
                buff.append("SET DEFAULT");
                break;
            case SET_NULL:
                buff.append("SET NULL");
                break;
            default:
                Message.throwInternalError("action=" + action);
        }
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be made.
     * 
     * @param table
     *            the table to create the object for
     * @param quotedName
     *            the name of this object (quoted if necessary)
     * @return the SQL statement
     */
    @Override
    public String getCreateSQLForCopy(final Table table, final String quotedName) {

        return getCreateSQLForCopy(table, refTable, quotedName, true);
    }

    /**
     * Create the SQL statement of this object so a copy of the table can be made.
     * 
     * @param table
     *            the table to create the object for
     * @param refTable
     *            the referenced table
     * @param quotedName
     *            the name of this object (quoted if necessary)
     * @param internalIndex
     *            add the index name to the statement
     * @return the SQL statement
     */
    public String getCreateSQLForCopy(final Table table, final Table refTable, final String quotedName, final boolean internalIndex) {

        final StringBuilder buff = new StringBuilder();
        buff.append("ALTER TABLE ");
        final String mainTable = table.getSQL();
        buff.append(mainTable);
        buff.append(" ADD CONSTRAINT ");
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        final IndexColumn[] cols = columns;
        final IndexColumn[] refCols = refColumns;
        buff.append(" FOREIGN KEY(");
        for (int i = 0; i < cols.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(cols[i].getSQL());
        }
        buff.append(")");
        if (internalIndex && indexOwner && table == this.table) {
            buff.append(" INDEX ");
            buff.append(index.getSQL());
        }
        buff.append(" REFERENCES ");
        String quotedRefTable;
        if (this.table == this.refTable) {
            // self-referencing constraints: need to use new table
            quotedRefTable = table.getSQL();
        }
        else {
            quotedRefTable = refTable.getSQL();
        }
        buff.append(quotedRefTable);
        buff.append("(");
        for (int i = 0; i < refCols.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(refCols[i].getSQL());
        }
        buff.append(")");
        if (internalIndex && refIndexOwner && table == this.table) {
            buff.append(" INDEX ");
            buff.append(refIndex.getSQL());
        }
        if (deleteAction != RESTRICT) {
            buff.append(" ON DELETE ");
            appendAction(buff, deleteAction);
        }
        if (updateAction != RESTRICT) {
            buff.append(" ON UPDATE ");
            appendAction(buff, updateAction);
        }
        buff.append(" NOCHECK");
        return buff.toString();
    }

    /**
     * Get a short description of the constraint. This includes the constraint name (if set), and the constraint expression.
     * 
     * @return the description
     */
    public String getShortDescription() {

        final StringBuilder buff = new StringBuilder();
        buff.append(getName());
        buff.append(": ");
        buff.append(table.getSQL());
        buff.append(" FOREIGN KEY(");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(")");
        buff.append(" REFERENCES ");
        buff.append(refTable.getSQL());
        buff.append("(");
        for (int i = 0; i < refColumns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(refColumns[i].getSQL());
        }
        buff.append(")");
        return buff.toString();
    }

    @Override
    public String getCreateSQLWithoutIndexes() {

        return getCreateSQLForCopy(table, refTable, getSQL(), false);
    }

    @Override
    public String getCreateSQL() {

        return getCreateSQLForCopy(table, getSQL());
    }

    public void setColumns(final IndexColumn[] cols) {

        columns = cols;
    }

    public IndexColumn[] getColumns() {

        return columns;
    }

    public void setRefColumns(final IndexColumn[] refCols) {

        refColumns = refCols;
    }

    public IndexColumn[] getRefColumns() {

        return refColumns;
    }

    public void setRefTable(final Table refTable) {

        this.refTable = refTable;
        if (refTable.getTemporary()) {
            setTemporary(true);
        }
    }

    /**
     * Set the index to use for this constraint.
     * 
     * @param index
     *            the index
     * @param isOwner
     *            true if the index is generated by the system and belongs to this constraint
     */
    public void setIndex(final Index index, final boolean isOwner) {

        this.index = index;
        indexOwner = isOwner;
    }

    /**
     * Set the index of the referenced table to use for this constraint.
     * 
     * @param refIndex
     *            the index
     * @param isRefOwner
     *            true if the index is generated by the system and belongs to this constraint
     */
    public void setRefIndex(final Index refIndex, final boolean isRefOwner) {

        this.refIndex = refIndex;
        refIndexOwner = isRefOwner;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        table.removeConstraint(this);
        refTable.removeConstraint(this);
        if (indexOwner) {
            table.removeIndexOrTransferOwnership(session, index);
        }
        if (refIndexOwner) {
            refTable.removeIndexOrTransferOwnership(session, refIndex);
        }
        database.removeMeta(session, getId());
        refTable = null;
        index = null;
        refIndex = null;
        columns = null;
        refColumns = null;
        deleteSQL = null;
        updateSQL = null;
        table = null;
        invalidate();
    }

    @Override
    public void checkRow(final Session session, final Table t, final Row oldRow, final Row newRow) throws SQLException {

        if (!database.getReferentialIntegrity()) { return; }
        if (!table.getCheckForeignKeyConstraints() || !refTable.getCheckForeignKeyConstraints()) { return; }
        if (t == table) {
            if (!skipOwnTable) {
                checkRowOwnTable(session, oldRow, newRow);
            }
        }
        if (t == refTable) {
            checkRowRefTable(session, oldRow, newRow);
        }
    }

    private void checkRowOwnTable(final Session session, final Row oldRow, final Row newRow) throws SQLException {

        if (newRow == null) { return; }
        boolean constraintColumnsEqual = oldRow != null;
        for (final IndexColumn column : columns) {
            final int idx = column.column.getColumnId();
            final Value v = newRow.getValue(idx);
            if (v == ValueNull.INSTANCE) {
                // return early if one of the columns is NULL
                return;
            }
            if (constraintColumnsEqual) {
                if (!v.compareEqual(oldRow.getValue(idx))) {
                    constraintColumnsEqual = false;
                }
            }
        }
        if (constraintColumnsEqual) {
            // return early if the key columns didn't change
            return;
        }
        if (refTable == table) {
            // special case self referencing constraints:
            // check the inserted row first
            boolean self = true;
            for (int i = 0; i < columns.length; i++) {
                final int idx = columns[i].column.getColumnId();
                final Value v = newRow.getValue(idx);
                final Column refCol = refColumns[i].column;
                final int refIdx = refCol.getColumnId();
                final Value r = newRow.getValue(refIdx);
                if (!database.areEqual(r, v)) {
                    self = false;
                    break;
                }
            }
            if (self) { return; }
        }
        final Row check = refTable.getTemplateRow();
        for (int i = 0; i < columns.length; i++) {
            final int idx = columns[i].column.getColumnId();
            final Value v = newRow.getValue(idx);
            final Column refCol = refColumns[i].column;
            final int refIdx = refCol.getColumnId();
            check.setValue(refIdx, v.convertTo(refCol.getType()));
        }
        if (!found(session, refIndex, check, null)) { throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, getShortDescription()); }
    }

    private boolean found(final Session session, final Index index, final SearchRow check, final Row excluding) throws SQLException {

        index.getTable().lock(session, false, false);
        final Cursor cursor = index.find(session, check, check);
        while (cursor.next()) {
            SearchRow found;
            found = cursor.getSearchRow();
            if (excluding != null && found.getPos() == excluding.getPos()) {
                continue;
            }
            final Column[] cols = index.getColumns();
            boolean allEqual = true;
            for (int i = 0; i < columns.length && i < cols.length; i++) {
                final int idx = cols[i].getColumnId();
                final Value c = check.getValue(idx);
                final Value f = found.getValue(idx);
                if (database.compareTypeSave(c, f) != 0) {
                    allEqual = false;
                    break;
                }
            }
            if (allEqual) { return true; }
        }
        return false;
    }

    private boolean isEqual(final Row oldRow, final Row newRow) throws SQLException {

        return refIndex.compareRows(oldRow, newRow) == 0;
    }

    private void checkRow(final Session session, final Row oldRow) throws SQLException {

        final SearchRow check = table.getTemplateSimpleRow(false);
        for (int i = 0; i < columns.length; i++) {
            final Column refCol = refColumns[i].column;
            final int refIdx = refCol.getColumnId();
            final Column col = columns[i].column;
            final int idx = col.getColumnId();
            final Value v = oldRow.getValue(refIdx).convertTo(col.getType());
            check.setValue(idx, v);
        }
        // exclude the row only for self-referencing constraints
        final Row excluding = refTable == table ? oldRow : null;
        if (found(session, index, check, excluding)) { throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1, getShortDescription()); }
    }

    private void checkRowRefTable(final Session session, final Row oldRow, final Row newRow) throws SQLException {

        if (oldRow == null) {
            // this is an insert
            return;
        }
        if (newRow != null && isEqual(oldRow, newRow)) {
            // on an update, if both old and new are the same, don't do anything
            return;
        }
        if (newRow == null) {
            // this is a delete
            if (deleteAction == RESTRICT) {
                checkRow(session, oldRow);
            }
            else {
                final int i = deleteAction == CASCADE ? 0 : columns.length;
                final Prepared deleteCommand = getDelete(session);
                setWhere(deleteCommand, i, oldRow);
                updateWithSkipCheck(deleteCommand);
            }
        }
        else {
            // this is an update
            if (updateAction == RESTRICT) {
                checkRow(session, oldRow);
            }
            else {
                final Prepared updateCommand = getUpdate(session);
                if (updateAction == CASCADE) {
                    final ObjectArray params = updateCommand.getParameters();
                    for (int i = 0; i < columns.length; i++) {
                        final Parameter param = (Parameter) params.get(i);
                        final Column refCol = refColumns[i].column;
                        param.setValue(newRow.getValue(refCol.getColumnId()));
                    }
                }
                setWhere(updateCommand, columns.length, oldRow);
                updateWithSkipCheck(updateCommand);
            }
        }
    }

    private void updateWithSkipCheck(final Prepared prep) throws SQLException {

        // TODO constraints: maybe delay the update or support delayed checks
        // (until commit)
        try {
            // TODO multithreaded kernel: this works only if nobody else updates
            // this or the ref table at the same time
            skipOwnTable = true;
            prep.setInternalQuery(true);
            try {
                prep.update();
            }
            catch (final RPCException e) {
                throw new SQLException(e.getMessage());
            }
        }
        finally {
            skipOwnTable = false;
        }
    }

    private void setWhere(final Prepared command, final int pos, final Row row) {

        for (int i = 0; i < refColumns.length; i++) {
            final int idx = refColumns[i].column.getColumnId();
            final Value v = row.getValue(idx);
            final ObjectArray params = command.getParameters();
            final Parameter param = (Parameter) params.get(pos + i);
            param.setValue(v);
        }
    }

    public int getDeleteAction() {

        return deleteAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on a delete.
     * 
     * @param action
     *            the action
     */
    public void setDeleteAction(final int action) throws SQLException {

        if (action == deleteAction && deleteSQL == null) { return; }
        if (deleteAction != RESTRICT) { throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON DELETE"); }
        deleteAction = action;
        buildDeleteSQL();
    }

    private void buildDeleteSQL() {

        if (deleteAction == RESTRICT) { return; }
        final StringBuilder buff = new StringBuilder();
        if (deleteAction == CASCADE) {
            buff.append("DELETE FROM ");
            buff.append(table.getSQL());
        }
        else {
            appendUpdate(buff);
        }
        appendWhere(buff);
        deleteSQL = buff.toString();
    }

    private Prepared getUpdate(final Session session) throws SQLException {

        return prepare(session, updateSQL, updateAction);
    }

    private Prepared getDelete(final Session session) throws SQLException {

        return prepare(session, deleteSQL, deleteAction);
    }

    public int getUpdateAction() {

        return updateAction;
    }

    /**
     * Set the action to apply (restrict, cascade,...) on an update.
     * 
     * @param action
     *            the action
     */
    public void setUpdateAction(final int action) throws SQLException {

        if (action == updateAction && updateSQL == null) { return; }
        if (updateAction != RESTRICT) { throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, "ON UPDATE"); }
        updateAction = action;
        buildUpdateSQL();
    }

    private void buildUpdateSQL() {

        if (updateAction == RESTRICT) { return; }
        final StringBuilder buff = new StringBuilder();
        appendUpdate(buff);
        appendWhere(buff);
        updateSQL = buff.toString();
    }

    @Override
    public void rebuild() throws SQLException {

        buildUpdateSQL();
        buildDeleteSQL();
    }

    private Prepared prepare(final Session session, final String sql, final int action) throws SQLException {

        final Prepared command = session.prepare(sql);
        if (action != CASCADE) {
            final ObjectArray params = command.getParameters();
            for (int i = 0; i < columns.length; i++) {
                final Column column = columns[i].column;
                final Parameter param = (Parameter) params.get(i);
                Value value;
                if (action == SET_NULL) {
                    value = ValueNull.INSTANCE;
                }
                else {
                    final Expression expr = column.getDefaultExpression();
                    if (expr == null) { throw Message.getSQLException(ErrorCode.NO_DEFAULT_SET_1, column.getName()); }
                    value = expr.getValue(session);
                }
                param.setValue(value);
            }
        }
        return command;
    }

    private void appendUpdate(final StringBuilder buff) {

        buff.append("UPDATE ");
        buff.append(table.getSQL());
        buff.append(" SET ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" , ");
            }
            final Column column = columns[i].column;
            buff.append(Parser.quoteIdentifier(column.getName()));
            buff.append("=?");
        }
    }

    private void appendWhere(final StringBuilder buff) {

        buff.append(" WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            final Column column = columns[i].column;
            buff.append(Parser.quoteIdentifier(column.getName()));
            buff.append("=?");
        }
    }

    @Override
    public Table getRefTable() {

        return refTable;
    }

    @Override
    public boolean usesIndex(final Index idx) {

        return idx == index || idx == refIndex;
    }

    @Override
    public void setIndexOwner(final Index index) {

        if (this.index == index) {
            indexOwner = true;
        }
        else if (refIndex == index) {
            refIndexOwner = true;
        }
        else {
            Message.throwInternalError();
        }
    }

    @Override
    public boolean containsColumn(final Column col) {

        for (final IndexColumn column : columns) {
            if (column.column == col) { return true; }
        }
        for (final IndexColumn refColumn : refColumns) {
            if (refColumn.column == col) { return true; }
        }
        return false;
    }

    @Override
    public boolean isBefore() {

        return false;
    }

    @Override
    public void checkExistingData(final Session session) throws SQLException {

        if (session.getDatabase().isStarting()) {
            // don't check at startup
            return;
        }
        final StringBuilder buff = new StringBuilder();
        buff.append("SELECT 1 FROM (SELECT ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(" FROM ");
        buff.append(table.getSQL());
        buff.append(" WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append(columns[i].getSQL());
            buff.append(" IS NOT NULL ");
        }
        buff.append(" ORDER BY ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            buff.append(columns[i].getSQL());
        }
        buff.append(") C WHERE NOT EXISTS(SELECT 1 FROM ");
        buff.append(refTable.getSQL());
        buff.append(" P WHERE ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                buff.append(" AND ");
            }
            buff.append("C.");
            buff.append(columns[i].getSQL());
            buff.append("=");
            buff.append("P.");
            buff.append(refColumns[i].getSQL());
        }
        buff.append(")");
        final String sql = buff.toString();
        final LocalResult r = session.prepare(sql).query(1);
        if (r.next()) { throw Message.getSQLException(ErrorCode.REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1, getShortDescription()); }
    }

    @Override
    public Index getUniqueIndex() {

        return refIndex;
    }

}
