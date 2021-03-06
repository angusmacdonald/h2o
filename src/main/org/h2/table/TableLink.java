/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.LinkedIndex;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.schema.Schema;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * A linked table contains connection information for a table accessible by JDBC. The table may be stored in a different database.
 */
public class TableLink extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100000;

    private String driver, url, user, password, originalSchema, originalTable, qualifiedTableName;

    private TableLinkConnection conn;

    private HashMap prepared = new HashMap();

    private final ObjectArray indexes = new ObjectArray();

    private final boolean emitUpdates;

    private LinkedIndex linkedIndex;

    private SQLException connectException;

    private boolean storesLowerCase;

    private boolean storesMixedCase;

    private boolean supportsMixedCaseIdentifiers;

    private boolean globalTemporary;

    private boolean readOnly;

    public TableLink(final Schema schema, final int id, final String name, final String driver, final String url, final String user, final String password, final String originalSchema, final String originalTable, final boolean emitUpdates, final boolean force) throws SQLException {

        super(schema, id, name, false);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.originalSchema = originalSchema;
        this.originalTable = originalTable;
        this.emitUpdates = emitUpdates;
        try {
            connect(false);
        }
        catch (final SQLException e) {
            connectException = e;
            if (!force) { throw e; }
            final Column[] cols = new Column[0];
            setColumns(cols);
            linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols), IndexType.createNonUnique(false));
            indexes.add(linkedIndex);
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Created new TableLink for " + name + " at " + url);
    }

    private void connect(final boolean clearLinkConnectionCache) throws SQLException {

        conn = database.getLinkConnection(driver, url, user, password, clearLinkConnectionCache);
        synchronized (conn) {
            try {
                readMetaData();
            }
            catch (final SQLException e) {

                if (!clearLinkConnectionCache) {
                    connect(true);
                }
                else {
                    conn.close();
                    conn = null;
                    throw e;
                }
            }
        }
    }

    private void readMetaData() throws SQLException {

        final DatabaseMetaData meta = conn.getConnection().getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
        ResultSet rs = meta.getTables(null, originalSchema, originalTable, null);
        // if (rs.next() && rs.next()) {
        // throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH,
        // originalTable);
        // }
        rs.close();

        if (originalTable.contains(".")) {
            final String[] split = originalTable.split("\\.");
            originalSchema = split[0];
            originalTable = split[1];
        }
        rs = meta.getColumns(null, originalSchema, originalTable, null);
        int i = 0;
        final ObjectArray columnList = new ObjectArray();
        final HashMap columnMap = new HashMap();
        String catalog = null, schema = null;
        while (rs.next()) {
            final String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            final String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                columnMap.clear();
                columnList.clear();
                break;
            }
            String n = rs.getString("COLUMN_NAME");

            n = convertColumnName(n);
            final int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            final int scale = rs.getInt("DECIMAL_DIGITS");
            final int displaySize = MathUtils.convertLongToInt(precision);
            final int type = DataType.convertSQLTypeToValueType(sqlType);
            final Column col = new Column(n, type, precision, scale, displaySize);
            if (rs.getString("COLUMN_DEF") == null) {
                col.setNullable(rs.getBoolean("IS_NULLABLE"));
            }
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        rs.close();
        if (originalTable.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
            qualifiedTableName = schema + "." + originalTable;
        }
        else {
            qualifiedTableName = originalTable;
        }
        // check if the table is accessible
        Statement stat = null;
        try {
            stat = conn.getConnection().createStatement();
            rs = stat.executeQuery("SELECT * FROM " + qualifiedTableName + " T WHERE 1=0[internal]");
            if (columnList.size() == 0) {
                // alternative solution
                final ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    final int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    final int scale = rsMeta.getScale(i + 1);
                    final int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    final int type = DataType.convertSQLTypeToValueType(sqlType);
                    final Column col = new Column(n, type, precision, scale, displaySize);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        }
        catch (final SQLException e) {
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new String[]{originalTable + "(" + e.toString() + ")"}, e);
        }
        finally {
            JdbcUtils.closeSilently(stat);
        }
        final Column[] cols = new Column[columnList.size()];
        columnList.toArray(cols);
        setColumns(cols);
        final int id = getId();
        linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols), IndexType.createNonUnique(false));
        indexes.add(linkedIndex);
        try {
            rs = meta.getPrimaryKeys(null, originalSchema, originalTable);
        }
        catch (final SQLException e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ObjectArray list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = new ObjectArray();
            do {
                final int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                final Column column = (Column) columnMap.get(col);
                list.set(idx - 1, column);
            }
            while (rs.next());
            addIndex(list, IndexType.createPrimaryKey(false, false));
            rs.close();
        }
        try {
            rs = meta.getIndexInfo(null, originalSchema, originalTable, false, true);
        }
        catch (final SQLException e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = new ObjectArray();
        IndexType indexType = null;
        if (rs != null) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
                final String newIndex = rs.getString("INDEX_NAME");
                if (pkName.equals(newIndex)) {
                    continue;
                }
                if (indexName != null && !indexName.equals(newIndex)) {
                    addIndex(list, indexType);
                    indexName = null;
                }
                if (indexName == null) {
                    indexName = newIndex;
                    list.clear();
                }
                final boolean unique = !rs.getBoolean("NON_UNIQUE");
                indexType = unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false);
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                final Column column = (Column) columnMap.get(col);
                list.add(column);
            }
            rs.close();
        }
        if (indexName != null) {
            addIndex(list, indexType);
        }
    }

    private long convertPrecision(final int sqlType, long precision) {

        // workaround for an Oracle problem
        // the precision reported by Oracle is 7 for a date column
        switch (sqlType) {
            case Types.DATE:
                precision = Math.max(ValueDate.PRECISION, precision);
                break;
            case Types.TIMESTAMP:
                precision = Math.max(ValueTimestamp.PRECISION, precision);
                break;
            case Types.TIME:
                precision = Math.max(ValueTime.PRECISION, precision);
                break;
        }
        return precision;
    }

    private String convertColumnName(String columnName) {

        if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private void addIndex(final ObjectArray list, final IndexType indexType) {

        final Column[] cols = new Column[list.size()];
        list.toArray(cols);
        final Index index = new LinkedIndex(this, 0, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
    }

    @Override
    public String getDropSQL() {

        return "DROP TABLE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("CREATE FORCE ");
        if (getTemporary()) {
            if (globalTemporary) {
                buff.append("GLOBAL ");
            }
            buff.append("TEMP ");
        }
        buff.append("LINKED TABLE ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(");
        buff.append(StringUtils.quoteStringSQL(driver));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(url));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(user));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(password));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(originalTable));
        buff.append(")");
        if (emitUpdates) {
            buff.append(" EMIT UPDATES");
        }
        if (readOnly) {
            buff.append(" READONLY");
        }
        return buff.toString();
    }

    @Override
    public Index addIndex(final Session session, final String indexName, final int indexId, final IndexColumn[] cols, final IndexType indexType, final int headPos, final String comment) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public Session lock(final Session session, final boolean exclusive, final boolean force) {

        return null;
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
    public Index getScanIndex(final Session session) {

        return linkedIndex;
    }

    private void checkReadOnly() throws SQLException {

        if (readOnly) { throw Message.getSQLException(ErrorCode.DATABASE_IS_READ_ONLY); }
    }

    @Override
    public void removeRow(final Session session, final Row row) throws SQLException {

        checkReadOnly();
        getScanIndex(session).remove(session, row);
    }

    @Override
    public void addRow(final Session session, final Row row) throws SQLException {

        checkReadOnly();
        getScanIndex(session).add(session, row);
    }

    @Override
    public void close(final Session session) throws SQLException {

        if (conn != null) {
            try {
                conn.close();
            }
            finally {
                conn = null;
            }
        }
    }

    @Override
    public synchronized long getRowCount(final Session session) throws SQLException {

        final String sql = "SELECT COUNT(*) FROM " + qualifiedTableName;
        try {
            final PreparedStatement prep = getPreparedStatement(sql, false);
            final ResultSet rs = prep.executeQuery();
            rs.next();
            final long count = rs.getLong(1);
            rs.close();
            return count;
        }
        catch (final SQLException e) {
            throw wrapException(sql, e);
        }
    }

    /**
     * Wrap a SQL exception that occurred while accessing a linked table.
     * 
     * @param sql
     *            the SQL statement
     * @param e
     *            the SQL exception from the remote database
     * @return the wrapped SQL exception
     */
    public SQLException wrapException(final String sql, final SQLException e) {

        return Message.getSQLException(ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2, new String[]{sql, e.toString() + ":" + url}, e);
    }

    public String getQualifiedTable() {

        return qualifiedTableName;
    }

    /**
     * Get a prepared statement object for the given statement. Prepared statements are kept in a hash map to avoid re-creating them.
     * 
     * @param sql
     *            the SQL statement
     * @param exclusive
     *            if the prepared statement must be removed from the map until reusePreparedStatement is called (only required for queries)
     * @return the prepared statement
     */
    public PreparedStatement getPreparedStatement(final String sql, final boolean exclusive) throws SQLException {

        final Trace trace = database.getTrace(Trace.TABLE);
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + ":\n" + sql);
        }
        if (conn == null) { throw connectException; }
        PreparedStatement prep = (PreparedStatement) prepared.get(sql);
        if (prep == null) {
            prep = conn.getConnection().prepareStatement(sql);
            prepared.put(sql, prep);
        }
        if (exclusive) {
            prepared.remove(sql);
        }
        return prep;
    }

    @Override
    public void unlock(final Session s) {

        // nothing to do
    }

    @Override
    public void checkRename() {

        // ok
    }

    @Override
    public void checkSupportAlter() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public boolean canGetRowCount() {

        return true;
    }

    @Override
    public boolean canDrop() {

        return true;
    }

    @Override
    public String getTableType() {

        return Table.TABLE_LINK;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        super.removeChildrenAndResources(session);
        close(session);
        database.removeMeta(session, getId());
        driver = null;
        url = user = password = originalTable = null;
        prepared = null;
        invalidate();
    }

    public boolean isOracle() {

        return url.startsWith("jdbc:oracle:");
    }

    @Override
    public ObjectArray getIndexes() {

        return indexes;
    }

    @Override
    public long getMaxDataModificationId() {

        // data may have been modified externally
        return Long.MAX_VALUE;
    }

    @Override
    public Index getUniqueIndex() {

        for (int i = 0; i < indexes.size(); i++) {
            final Index idx = (Index) indexes.get(i);
            if (idx.getIndexType().getUnique()) { return idx; }
        }
        return null;
    }

    @Override
    public void updateRows(final Prepared prepared, final Session session, final RowList rows) throws SQLException {

        boolean deleteInsert;
        checkReadOnly();
        if (emitUpdates) {
            for (rows.reset(); rows.hasNext();) {
                prepared.checkCanceled();
                final Row oldRow = rows.next();
                final Row newRow = rows.next();
                linkedIndex.update(oldRow, newRow);
                session.log(this, UndoLogRecord.DELETE, oldRow);
                session.log(this, UndoLogRecord.INSERT, newRow);
            }
            deleteInsert = false;
        }
        else {
            deleteInsert = true;
        }
        if (deleteInsert) {
            super.updateRows(prepared, session, rows);
        }
    }

    @Override
    public void setGlobalTemporary(final boolean globalTemporary) {

        this.globalTemporary = globalTemporary;
    }

    public void setReadOnly(final boolean readOnly) {

        this.readOnly = readOnly;
    }

    public TableLinkConnection getConnection() {

        return conn;
    }

    @Override
    public long getRowCountApproximation() {

        return ROW_COUNT_APPROXIMATION;
    }

    /**
     * Add this prepared statement to the list of cached statements.
     * 
     * @param prep
     *            the prepared statement
     * @param sql
     *            the SQL statement
     */
    public void reusePreparedStatement(final PreparedStatement prep, final String sql) {

        prepared.put(sql, prep);
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#isLocal()
     */
    @Override
    public boolean isLocal() {

        return false;
    }

    public String getUrl() {

        return url;
    }
}
