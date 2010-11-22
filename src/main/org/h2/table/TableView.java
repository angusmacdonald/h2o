/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.ViewIndex;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.IntArray;
import org.h2.util.ObjectArray;
import org.h2.util.SmallLRUCache;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A view is a virtual table that is defined by a query.
 */
public class TableView extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100;

    private String querySQL;

    private ObjectArray tables;

    private final String[] columnNames;

    private Query viewQuery;

    private ViewIndex index;

    private boolean recursive;

    private SQLException createException;

    private final SmallLRUCache indexCache = new SmallLRUCache(Constants.VIEW_INDEX_CACHE_SIZE);

    private long lastModificationCheck;

    private long maxDataModificationId;

    private User owner;

    private Query topQuery;

    public List<Table> getTables() {

        final List<Table> ts = new LinkedList<Table>();

        final Table[] tablesAndViews = new Table[tables.size()];
        tables.toArray(tablesAndViews);

        for (final Table tableOrView : tablesAndViews) {
            if (tableOrView != null && Table.TABLE.equals(tableOrView.getTableType())) {
                ts.add(tableOrView); // it's a table.
            }
            else if (tableOrView != null && Table.VIEW.equals(tableOrView.getTableType())) {
                // recurse - its a view.
                final TableView view = (TableView) tableOrView;
                ts.addAll(view.getTables());
            }
        }

        return ts;

    }

    public TableView(final Schema schema, final int id, final String name, final String querySQL, final ObjectArray params, final String[] columnNames, final Session session, final boolean recursive) throws SQLException {

        super(schema, id, name, false);
        this.querySQL = querySQL;
        this.columnNames = columnNames;
        this.recursive = recursive;
        index = new ViewIndex(this, querySQL, params, recursive);
        initColumnsAndTables(session);
    }

    /**
     * Re-compile the query, updating the SQL statement.
     * 
     * @param session
     *            the session
     * @return the query
     */
    public Query recompileQuery(final Session session) throws SQLException {

        final Prepared p = session.prepare(querySQL);
        if (!(p instanceof Query)) { throw Message.getSyntaxError(querySQL, 0); }
        final Query query = (Query) p;
        querySQL = query.getPlanSQL();
        return query;
    }

    private void initColumnsAndTables(final Session session) throws SQLException {

        Column[] cols;
        removeViewFromTables();
        try {
            final Query query = recompileQuery(session);
            tables = new ObjectArray(query.getTables());
            final ObjectArray expressions = query.getExpressions();
            final ObjectArray list = new ObjectArray();
            for (int i = 0; i < query.getColumnCount(); i++) {
                final Expression expr = (Expression) expressions.get(i);
                String name = null;
                if (columnNames != null && columnNames.length > i) {
                    name = columnNames[i];
                }
                if (name == null) {
                    name = expr.getAlias();
                }
                final int type = expr.getType();
                final long precision = expr.getPrecision();
                final int scale = expr.getScale();
                final int displaySize = expr.getDisplaySize();
                final Column col = new Column(name, type, precision, scale, displaySize);
                col.setTable(this, i);
                list.add(col);
            }
            cols = new Column[list.size()];
            list.toArray(cols);
            createException = null;
            viewQuery = query;
        }
        catch (final SQLException e) {
            createException = e;
            // if it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database
            tables = new ObjectArray();
            cols = new Column[0];
            if (recursive && columnNames != null) {
                cols = new Column[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    cols[i] = new Column(columnNames[i], Value.STRING);
                }
                index.setRecursive(true);
                recursive = true;
                createException = null;
            }

        }
        setColumns(cols);
        if (getId() != 0) {
            addViewToTables();
        }
    }

    /**
     * Check if this view is currently invalid.
     * 
     * @return true if it is
     */
    public boolean getInvalid() {

        return createException != null;
    }

    @Override
    public PlanItem getBestPlanItem(final Session session, final int[] masks) throws SQLException {

        final PlanItem item = new PlanItem();
        item.cost = index.getCost(session, masks);
        final IntArray masksArray = new IntArray(masks == null ? new int[0] : masks);
        ViewIndex i2 = (ViewIndex) indexCache.get(masksArray);
        if (i2 == null || i2.getSession() != session) {
            i2 = new ViewIndex(this, index, session, masks);
            indexCache.put(masksArray, i2);
        }
        item.setIndex(i2);
        return item;
    }

    @Override
    public String getDropSQL() {

        return "DROP VIEW IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("CREATE FORCE VIEW ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        if (columns.length > 0) {
            buff.append('(');
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(columns[i].getSQL());
            }
            buff.append(")");
        }
        else if (columnNames != null) {
            buff.append('(');
            for (int i = 0; i < columnNames.length; i++) {
                if (i > 0) {
                    buff.append(", ");
                }
                buff.append(columnNames[i]);
            }
            buff.append(")");
        }
        buff.append(" AS\n");
        buff.append(querySQL);
        return buff.toString();
    }

    @Override
    public void checkRename() {

        // ok
    }

    @Override
    public Session lock(final Session session, final boolean exclusive, final boolean force) {

        return null;
        // exclusive lock means: the view will be dropped
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
    public boolean isLockedExclusivelyBy(final Session session) {

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

        // TODO view: alter what? rename is ok
        throw Message.getUnsupportedException();
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public long getRowCount(final Session session) {

        throw Message.throwInternalError();
    }

    @Override
    public boolean canGetRowCount() {

        // TODO view: could get the row count, but not that easy
        return false;
    }

    @Override
    public boolean canDrop() {

        return true;
    }

    @Override
    public String getTableType() {

        return Table.VIEW;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        removeViewFromTables();
        super.removeChildrenAndResources(session);
        database.removeMeta(session, getId());
        querySQL = null;
        index = null;
        invalidate();
    }

    @Override
    public String getSQL() {

        if (getTemporary()) {
            final StringBuilder buff = new StringBuilder(querySQL.length());
            buff.append("(");
            buff.append(querySQL);
            buff.append(")");
            return buff.toString();
        }
        return super.getSQL();
    }

    @Override
    public Index getScanIndex(final Session session) throws SQLException {

        if (createException != null) {
            final String msg = createException.getMessage();
            throw Message.getSQLException(ErrorCode.VIEW_IS_INVALID_2, new String[]{getSQL(), msg}, createException);
        }
        final PlanItem item = getBestPlanItem(session, null);
        return item.getIndex();
    }

    @Override
    public ObjectArray getIndexes() {

        return null;
    }

    /**
     * Re-compile the view query.
     * 
     * @param session
     *            the session
     */
    public void recompile(final Session session) throws SQLException {

        for (int i = 0; i < tables.size(); i++) {
            final Table t = (Table) tables.get(i);
            t.removeView(this);
        }
        tables.clear();
        initColumnsAndTables(session);
    }

    @Override
    public long getMaxDataModificationId() {

        if (createException != null) { return Long.MAX_VALUE; }
        if (viewQuery == null) { return Long.MAX_VALUE; }
        // if nothing was modified in the database since the last check, and the
        // last is known, then we don't need to check again
        // this speeds up nested views
        final long dbMod = database.getModificationDataId();
        if (dbMod > lastModificationCheck && maxDataModificationId <= dbMod) {
            maxDataModificationId = viewQuery.getMaxDataModificationId();
            lastModificationCheck = dbMod;
        }
        return maxDataModificationId;
    }

    @Override
    public Index getUniqueIndex() {

        return null;
    }

    private void removeViewFromTables() {

        if (tables != null) {
            for (int i = 0; i < tables.size(); i++) {
                final Table t = (Table) tables.get(i);
                t.removeView(this);
            }
            tables.clear();
        }
    }

    private void addViewToTables() {

        for (int i = 0; i < tables.size(); i++) {
            final Table t = (Table) tables.get(i);
            t.addView(this);
        }
    }

    private void setOwner(final User owner) {

        this.owner = owner;
    }

    public User getOwner() {

        return owner;
    }

    /**
     * Create a temporary view out of the given query.
     * 
     * @param session
     *            the session
     * @param owner
     *            the owner of the query
     * @param name
     *            the view name
     * @param query
     *            the query
     * @param topQuery
     *            the top level query
     * @return the view table
     */
    public static TableView createTempView(final Session session, final User owner, final String name, final Query query, final Query topQuery) throws SQLException {

        final Schema mainSchema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        final String querySQL = query.getPlanSQL();
        final TableView v = new TableView(mainSchema, 0, name, querySQL, query.getParameters(), null, session, false);
        v.setTopQuery(topQuery);
        if (v.createException != null) { throw v.createException; }
        v.setOwner(owner);
        v.setTemporary(true);
        return v;
    }

    private void setTopQuery(final Query topQuery) {

        this.topQuery = topQuery;
    }

    @Override
    public long getRowCountApproximation() {

        return ROW_COUNT_APPROXIMATION;
    }

    public int getParameterOffset() {

        return topQuery == null ? 0 : topQuery.getParameters().size();
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
