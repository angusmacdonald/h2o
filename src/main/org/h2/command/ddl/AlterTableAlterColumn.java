/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2o.db.query.TableProxy;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.locking.LockRequest;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statements ALTER TABLE ADD, ALTER TABLE ALTER COLUMN, ALTER TABLE ALTER COLUMN RESTART, ALTER TABLE ALTER
 * COLUMN SELECTIVITY, ALTER TABLE ALTER COLUMN SET DEFAULT, ALTER TABLE ALTER COLUMN SET NOT NULL, ALTER TABLE ALTER COLUMN SET NULL, ALTER
 * TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaCommand {

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NOT NULL statement.
     */
    public static final int NOT_NULL = 0;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET NULL statement.
     */
    public static final int NULL = 1;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SET DEFAULT statement.
     */
    public static final int DEFAULT = 2;

    /**
     * The type of a ALTER TABLE ALTER COLUMN statement that changes the column data type.
     */
    public static final int CHANGE_TYPE = 3;

    /**
     * The type of a ALTER TABLE ADD statement.
     */
    public static final int ADD = 4;

    /**
     * The type of a ALTER TABLE DROP COLUMN statement.
     */
    public static final int DROP = 5;

    /**
     * The type of a ALTER TABLE ALTER COLUMN SELECTIVITY statement.
     */
    public static final int SELECTIVITY = 6;

    private Column oldColumn;

    private Column newColumn;

    private int type;

    private Expression defaultExpression;

    private Expression newSelectivity;

    private String addBefore;

    public AlterTableAlterColumn(final Session session, final Schema schema, final boolean internalQuery) {

        super(session, schema);
        setInternalQuery(internalQuery);
    }

    public void setOldColumn(final Column oldColumn) {

        this.oldColumn = oldColumn;
    }

    public void setAddBefore(final String before) {

        addBefore = before;
    }

    @Override
    public int update(final String transactionName) throws SQLException, RemoteException {

        session.commit(true);

        /*
         * (QUERY PROPAGATED TO ALL REPLICAS).
         */
        if (isRegularTable()) {
            if (tableProxy == null) {
                tableProxy = TableProxy.getTableProxyAndLock(table, LockType.WRITE, new LockRequest(session), session.getDatabase());
            }

            return tableProxy.executeUpdate(sqlStatement, transactionName, session);
        }

        final Database db = session.getDatabase();
        session.getUser().checkRight(table, Right.ALL);
        table.checkSupportAlter();
        table.lock(session, true, true);
        final Sequence sequence = oldColumn == null ? null : oldColumn.getSequence();
        switch (type) {
            case NOT_NULL: {
                if (!oldColumn.getNullable()) {
                    // no change
                    break;
                }
                checkNoNullValues();
                oldColumn.setNullable(false);
                db.update(session, table);
                break;
            }
            case NULL: {
                if (oldColumn.getNullable()) {
                    // no change
                    break;
                }
                checkNullable();
                oldColumn.setNullable(true);
                db.update(session, table);
                break;
            }
            case DEFAULT: {
                oldColumn.setSequence(null);
                oldColumn.setDefaultExpression(session, defaultExpression);
                removeSequence(sequence);
                db.update(session, table);
                break;
            }
            case CHANGE_TYPE: {
                // TODO document data type change problems when used with
                // autoincrement columns.
                // sequence will be unlinked
                checkNoViews();
                oldColumn.setSequence(null);
                oldColumn.setDefaultExpression(session, null);
                oldColumn.setConvertNullToDefault(false);
                if (oldColumn.getNullable() && !newColumn.getNullable()) {
                    checkNoNullValues();
                }
                else if (!oldColumn.getNullable() && newColumn.getNullable()) {
                    checkNullable();
                }
                convertToIdentityIfRequired(newColumn);
                copyData();
                break;
            }
            case ADD: {
                checkNoViews();
                convertToIdentityIfRequired(newColumn);
                copyData();
                break;
            }
            case DROP: {
                checkNoViews();
                if (table.getColumns().length == 1) { throw Message.getSQLException(ErrorCode.CANNOT_DROP_LAST_COLUMN, oldColumn.getSQL()); }
                table.checkColumnIsNotReferenced(oldColumn);
                dropSingleColumnIndexes();
                copyData();
                break;
            }
            case SELECTIVITY: {
                final int value = newSelectivity.optimize(session).getValue(session).getInt();
                oldColumn.setSelectivity(value);
                db.update(session, table);
                break;
            }
            default:
                Message.throwInternalError("type=" + type);
        }
        return 0;
    }

    private void convertToIdentityIfRequired(final Column c) {

        if (c.getAutoIncrement()) {
            c.setOriginalSQL("IDENTITY");
        }
    }

    private void removeSequence(final Sequence sequence) throws SQLException {

        if (sequence != null) {
            table.removeSequence(session, sequence);
            sequence.setBelongsToTable(false);
            final Database db = session.getDatabase();
            db.removeSchemaObject(session, sequence);
        }
    }

    private void checkNoViews() throws SQLException {

        final ObjectArray children = table.getChildren();
        for (int i = 0; i < children.size(); i++) {
            final DbObject child = (DbObject) children.get(i);
            if (child.getType() == DbObject.TABLE_OR_VIEW) { throw Message.getSQLException(ErrorCode.OPERATION_NOT_SUPPORTED_WITH_VIEWS_2, new String[]{table.getName(), child.getName()}); }
        }
    }

    private void copyData() throws SQLException, RemoteException {

        final Database db = session.getDatabase();
        final String tempName = db.getTempTableName(session.getSessionId());
        final Column[] columns = table.getColumns();
        final ObjectArray newColumns = new ObjectArray();
        for (final Column column : columns) {
            final Column col = column.getClone();
            newColumns.add(col);
        }
        if (type == DROP) {
            final int position = oldColumn.getColumnId();
            newColumns.remove(position);
        }
        else if (type == ADD) {
            int position;
            if (addBefore == null) {
                position = columns.length;
            }
            else {
                position = table.getColumn(addBefore).getColumnId();
            }
            newColumns.add(position, newColumn);
        }
        else if (type == CHANGE_TYPE) {
            final int position = oldColumn.getColumnId();
            newColumns.remove(position);
            newColumns.add(position, newColumn);
        }
        final boolean persistent = table.getPersistent();
        // create a table object in order to get the SQL statement
        // can't just use this table, because most column objects are 'shared'
        // with the old table
        // still need a new id because using 0 would mean: the new table tries
        // to use the rows of the table 0 (the meta table)
        final int id = -1;
        TableData newTable = getSchema().createTable(tempName, id, newColumns, persistent, false, Index.EMPTY_HEAD);
        newTable.setComment(table.getComment());
        final StringBuilder buff = new StringBuilder(newTable.getCreateSQL());
        final StringBuilder columnList = new StringBuilder();
        for (int i = 0; i < newColumns.size(); i++) {
            final Column nc = (Column) newColumns.get(i);
            if (columnList.length() > 0) {
                columnList.append(", ");
            }
            if (type == ADD && nc == newColumn) {
                final Expression def = nc.getDefaultExpression();
                columnList.append(def == null ? "NULL" : def.getSQL());
            }
            else {
                columnList.append(nc.getSQL());
            }
        }
        buff.append(" AS SELECT ");
        if (columnList.length() == 0) {
            // special case insert into test select * from test
            buff.append("*");
        }
        else {
            buff.append(columnList);
        }
        buff.append(" FROM ");
        buff.append(table.getSQL());
        final String newTableSQL = buff.toString();
        execute(newTableSQL, true);
        newTable = (TableData) newTable.getSchema().getTableOrView(session, newTable.getName());
        ObjectArray children = table.getChildren();
        final ObjectArray triggers = new ObjectArray();
        for (int i = 0; i < children.size(); i++) {
            final DbObject child = (DbObject) children.get(i);
            if (child instanceof Sequence) {
                continue;
            }
            else if (child instanceof Index) {
                final Index idx = (Index) child;
                if (idx.getIndexType().getBelongsToConstraint()) {
                    continue;
                }
            }
            final String createSQL = child.getCreateSQL();
            if (createSQL == null) {
                continue;
            }
            if (child.getType() == DbObject.TABLE_OR_VIEW) {
                Message.throwInternalError();
            }
            final String quotedName = Parser.quoteIdentifier(tempName + "_" + child.getName());
            String sql = null;
            if (child instanceof ConstraintReferential) {
                final ConstraintReferential r = (ConstraintReferential) child;
                if (r.getTable() != table) {
                    sql = r.getCreateSQLForCopy(r.getTable(), newTable, quotedName, false);
                }
            }
            if (sql == null) {
                sql = child.getCreateSQLForCopy(newTable, quotedName);
            }
            if (sql != null) {
                if (child instanceof TriggerObject) {
                    triggers.add(sql);
                }
                else {
                    execute(sql, true);
                }
            }
        }
        final String tableName = table.getName();
        table.setModified();
        for (final Column column : columns) {
            // if we don't do that, the sequence is dropped when the table is
            // dropped
            final Sequence seq = column.getSequence();
            if (seq != null) {
                table.removeSequence(session, seq);
                column.setSequence(null);
            }
        }
        for (int i = 0; i < triggers.size(); i++) {
            final String sql = (String) triggers.get(i);
            execute(sql, true);
        }
        execute("DROP TABLE " + table.getSQL(), true);
        db.renameSchemaObject(session, newTable, tableName);
        children = newTable.getChildren();
        for (int i = 0; i < children.size(); i++) {
            final DbObject child = (DbObject) children.get(i);
            if (child instanceof Sequence) {
                continue;
            }
            String name = child.getName();
            if (name == null || child.getCreateSQL() == null) {
                continue;
            }
            if (name.startsWith(tempName + "_")) {
                name = name.substring(tempName.length() + 1);
                db.renameSchemaObject(session, (SchemaObject) child, name);
            }
        }
    }

    private void execute(final String sql, final boolean ddl) throws SQLException, RemoteException {

        final Prepared command = session.prepare(sql);
        command.update();
    }

    private void dropSingleColumnIndexes() throws SQLException {

        final Database db = session.getDatabase();
        ObjectArray indexes = table.getIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            final Index index = (Index) indexes.get(i);
            if (index.getCreateSQL() == null) {
                continue;
            }
            boolean dropIndex = false;
            final Column[] cols = index.getColumns();
            for (final Column col : cols) {
                if (col == oldColumn) {
                    if (cols.length == 1) {
                        dropIndex = true;
                    }
                    else {
                        throw Message.getSQLException(ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL());
                    }
                }
            }
            if (dropIndex) {
                db.removeSchemaObject(session, index);
                indexes = table.getIndexes();
                i = -1;
            }
        }
    }

    private void checkNullable() throws SQLException {

        final ObjectArray indexes = table.getIndexes();
        for (int i = 0; i < indexes.size(); i++) {
            final Index index = (Index) indexes.get(i);
            if (index.getColumnIndex(oldColumn) < 0) {
                continue;
            }
            final IndexType indexType = index.getIndexType();
            if (indexType.getPrimaryKey() || indexType.getHash()) { throw Message.getSQLException(ErrorCode.COLUMN_IS_PART_OF_INDEX_1, index.getSQL()); }
        }
    }

    private void checkNoNullValues() throws SQLException {

        final String sql = "SELECT COUNT(*) FROM " + table.getSQL() + " WHERE " + oldColumn.getSQL() + " IS NULL";
        final Prepared command = session.prepare(sql);
        final LocalResult result = command.query(0);
        result.next();
        if (result.currentRow()[0].getInt() > 0) { throw Message.getSQLException(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, oldColumn.getSQL()); }
    }

    public void setType(final int type) {

        this.type = type;
    }

    public void setSelectivity(final Expression selectivity) {

        newSelectivity = selectivity;
    }

    public void setDefaultExpression(final Expression defaultExpression) {

        this.defaultExpression = defaultExpression;
    }

    public void setNewColumn(final Column newColumn) {

        this.newColumn = newColumn;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#acquireLocks()
     */
    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        acquireLocks(tableProxyManager, table, LockType.WRITE);

    }

    /**
     * True if the table involved in the prepared statement is a regular table - i.e. not an H2O meta-data table.
     */
    @Override
    protected boolean isRegularTable() {

        final boolean isLocal = session.getDatabase().isTableLocal(getSchema());
        return !session.getDatabase().isManagementDB() && !isStartup() && !internalQuery && !isLocal;

    }
}
