/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.tools;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;

import org.h2.message.Message;

/**
 * This class is a simple result set and meta data implementation. It can be used in Java functions that return a result set. Only the most
 * basic methods are implemented, the others throw an exception. This implementation is standalone, and only relies on standard classes. It
 * can be extended easily if required.
 * 
 * An application can create a result set using the following code:
 * 
 * <pre>
 * SimpleResultSet rs = new SimpleResultSet();
 * rs.addColumn(&quot;ID&quot;, Types.INTEGER, 10, 0);
 * rs.addColumn(&quot;NAME&quot;, Types.VARCHAR, 255, 0);
 * rs.addRow(new Object[] { new Integer(0), &quot;Hello&quot; });
 * rs.addRow(new Object[] { new Integer(1), &quot;World&quot; });
 * </pre>
 * 
 */
public class SimpleResultSet implements ResultSet, ResultSetMetaData {

    private ArrayList rows;

    private Object[] currentRow;

    private int rowId = -1;

    private boolean wasNull;

    private SimpleRowSource source;

    private ArrayList columns = new ArrayList();

    /**
     * This class holds the data of a result column.
     */
    static class Column {

        /**
         * The column name.
         */
        String name;

        /**
         * The SQL type.
         */
        int sqlType;

        /**
         * The precision.
         */
        int precision;

        /**
         * The scale.
         */
        int scale;
    }

    /**
     * A simple array implementation, backed by an object array
     */
    private static class SimpleArray implements Array {

        private final Object[] value;

        SimpleArray(final Object[] value) {

            this.value = value;
        }

        /**
         * Get the object array.
         * 
         * @return the object array
         */
        @Override
        public Object getArray() {

            return value;
        }

        /**
         * INTERNAL
         */
        @Override
        public Object getArray(final Map map) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public Object getArray(final long index, final int count) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public Object getArray(final long index, final int count, final Map map) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * Get the base type of this array.
         * 
         * @return Types.NULL
         */
        @Override
        public int getBaseType() {

            return Types.NULL;
        }

        /**
         * Get the base type name of this array.
         * 
         * @return "NULL"
         */
        @Override
        public String getBaseTypeName() {

            return "NULL";
        }

        /**
         * INTERNAL
         */
        @Override
        public ResultSet getResultSet() throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public ResultSet getResultSet(final Map map) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public ResultSet getResultSet(final long index, final int count) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public ResultSet getResultSet(final long index, final int count, final Map map) throws SQLException {

            throw getUnsupportedException();
        }

        /**
         * INTERNAL
         */
        @Override
        public void free() {

            // nothing to do
        }

    }

    /**
     * This constructor is used if the result set is later populated with addRow.
     */
    public SimpleResultSet() {

        rows = new ArrayList();
    }

    /**
     * This constructor is used if the result set should retrieve the rows using the specified row source object.
     * 
     * @param source
     *            the row source
     */
    public SimpleResultSet(final SimpleRowSource source) {

        this.source = source;
    }

    /**
     * Adds a column to the result set.
     * 
     * @param name
     *            null is replaced with C1, C2,...
     * @param sqlType
     *            the value returned in getColumnType(..) (ignored internally)
     * @param precision
     *            the precision
     * @param scale
     *            the scale
     */
    public void addColumn(String name, final int sqlType, final int precision, final int scale) throws SQLException {

        if (rows != null && rows.size() > 0) { throw new SQLException("Cannot add a column after adding rows", "21S02"); }
        if (name == null) {
            name = "C" + (columns.size() + 1);
        }
        final Column column = new Column();
        column.name = name;
        column.sqlType = sqlType;
        column.precision = precision;
        column.scale = scale;
        columns.add(column);
    }

    /**
     * Add a new row to the result set.
     * 
     * @param row
     *            the row as an array of objects
     */
    public void addRow(final Object[] row) throws SQLException {

        if (rows == null) { throw new SQLException("Cannot add a row when using RowSource", "21S02"); }
        rows.add(row);
    }

    /**
     * Returns ResultSet.CONCUR_READ_ONLY.
     * 
     * @return CONCUR_READ_ONLY
     */
    @Override
    public int getConcurrency() {

        return ResultSet.CONCUR_READ_ONLY;
    }

    /**
     * Returns ResultSet.FETCH_FORWARD.
     * 
     * @return FETCH_FORWARD
     */
    @Override
    public int getFetchDirection() {

        return ResultSet.FETCH_FORWARD;
    }

    /**
     * Returns 0.
     * 
     * @return 0
     */
    @Override
    public int getFetchSize() {

        return 0;
    }

    /**
     * Returns the row number (1, 2,...) or 0 for no row.
     * 
     * @return 0
     */
    @Override
    public int getRow() {

        return rowId + 1;
    }

    /**
     * Returns ResultSet.TYPE_FORWARD_ONLY.
     * 
     * @return TYPE_FORWARD_ONLY
     */
    @Override
    public int getType() {

        return ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * Closes the result set and releases the resources.
     */
    @Override
    public void close() {

        currentRow = null;
        rows = null;
        columns = null;
        rowId = -1;
        if (source != null) {
            source.close();
            source = null;
        }
    }

    /**
     * Moves the cursor to the next row of the result set.
     * 
     * @return true if successful, false if there are no more rows
     */
    @Override
    public boolean next() throws SQLException {

        if (source != null) {
            rowId++;
            currentRow = source.readRow();
            if (currentRow != null) { return true; }
        }
        else if (rows != null && rowId < rows.size()) {
            rowId++;
            if (rowId < rows.size()) {
                currentRow = (Object[]) rows.get(rowId);
                return true;
            }
        }
        close();
        return false;
    }

    /**
     * Moves the current position to before the first row, that means resets the result set.
     */
    @Override
    public void beforeFirst() throws SQLException {

        rowId = -1;
        if (source != null) {
            source.reset();
        }
    }

    /**
     * Returns whether the last column accessed was null.
     * 
     * @return true if the last column accessed was null
     */
    @Override
    public boolean wasNull() {

        return wasNull;
    }

    /**
     * Returns the value as a byte.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public byte getByte(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) {
            o = Byte.decode(o.toString());
        }
        return o == null ? 0 : ((Number) o).byteValue();
    }

    /**
     * Returns the value as an double.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public double getDouble(final int columnIndex) throws SQLException {

        final Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) { return Double.parseDouble(o.toString()); }
        return o == null ? 0 : ((Number) o).doubleValue();
    }

    /**
     * Returns the value as a float.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public float getFloat(final int columnIndex) throws SQLException {

        final Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) { return Float.parseFloat(o.toString()); }
        return o == null ? 0 : ((Number) o).floatValue();
    }

    /**
     * Returns the value as an int.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public int getInt(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) {
            o = Integer.decode(o.toString());
        }
        return o == null ? 0 : ((Number) o).intValue();
    }

    /**
     * Returns the value as a long.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public long getLong(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) {
            o = Long.decode(o.toString());
        }
        return o == null ? 0 : ((Number) o).longValue();
    }

    /**
     * Returns the value as a short.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public short getShort(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof Number)) {
            o = Short.decode(o.toString());
        }
        return o == null ? 0 : ((Number) o).shortValue();
    }

    /**
     * Returns the value as a boolean.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof Boolean)) {
            o = Boolean.valueOf(o.toString());
        }
        return o == null ? false : ((Boolean) o).booleanValue();
    }

    /**
     * Returns the value as a byte array.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {

        return (byte[]) get(columnIndex);
    }

    /**
     * Returns the value as an Object.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public Object getObject(final int columnIndex) throws SQLException {

        return get(columnIndex);
    }

    /**
     * Returns the value as a String.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public String getString(final int columnIndex) throws SQLException {

        final Object o = get(columnIndex);
        return o == null ? null : o.toString();
    }

    /**
     * Returns the value as a byte.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public byte getByte(final String columnName) throws SQLException {

        return getByte(findColumn(columnName));
    }

    /**
     * Returns the value as a double.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public double getDouble(final String columnName) throws SQLException {

        return getDouble(findColumn(columnName));
    }

    /**
     * Returns the value as a float.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public float getFloat(final String columnName) throws SQLException {

        return getFloat(findColumn(columnName));
    }

    /**
     * Searches for a specific column in the result set. A case-insensitive search is made.
     * 
     * @param columnName
     *            the name of the column label
     * @return the column index (1,2,...)
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public int findColumn(final String columnName) throws SQLException {

        for (int i = 0; columnName != null && columns != null && i < columns.size(); i++) {
            if (columnName.equalsIgnoreCase(getColumn(i).name)) { return i + 1; }
        }
        throw new SQLException("Column not found: " + columnName, "42S22");
    }

    /**
     * Returns the value as an int.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public int getInt(final String columnName) throws SQLException {

        return getInt(findColumn(columnName));
    }

    /**
     * Returns the value as a long.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public long getLong(final String columnName) throws SQLException {

        return getLong(findColumn(columnName));
    }

    /**
     * Returns the value as a short.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public short getShort(final String columnName) throws SQLException {

        return getShort(findColumn(columnName));
    }

    /**
     * Returns the value as a boolean.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public boolean getBoolean(final String columnName) throws SQLException {

        return getBoolean(findColumn(columnName));
    }

    /**
     * Returns the value as a byte array.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public byte[] getBytes(final String columnName) throws SQLException {

        return getBytes(findColumn(columnName));
    }

    /**
     * Returns the value as a java.math.BigDecimal.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {

        Object o = get(columnIndex);
        if (o != null && !(o instanceof BigDecimal)) {
            o = new BigDecimal(o.toString());
        }
        return (BigDecimal) o;
    }

    /**
     * Returns the value as an java.sql.Date.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public Date getDate(final int columnIndex) throws SQLException {

        return (Date) get(columnIndex);
    }

    /**
     * Returns a reference to itself.
     * 
     * @return this
     */
    @Override
    public ResultSetMetaData getMetaData() {

        return this;
    }

    /**
     * Returns null.
     * 
     * @return null
     */
    @Override
    public SQLWarning getWarnings() {

        return null;
    }

    /**
     * Returns null.
     * 
     * @return null
     */
    @Override
    public Statement getStatement() {

        return null;
    }

    /**
     * Returns the value as an java.sql.Time.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public Time getTime(final int columnIndex) throws SQLException {

        return (Time) get(columnIndex);
    }

    /**
     * Returns the value as an java.sql.Timestamp.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {

        return (Timestamp) get(columnIndex);
    }

    /**
     * Returns the value as a java.sql.Array.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     */
    @Override
    public Array getArray(final int columnIndex) throws SQLException {

        return new SimpleArray((Object[]) get(columnIndex));
    }

    /**
     * Returns the value as an Object.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public Object getObject(final String columnName) throws SQLException {

        return getObject(findColumn(columnName));
    }

    /**
     * Returns the value as a String.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public String getString(final String columnName) throws SQLException {

        return getString(findColumn(columnName));
    }

    /**
     * Returns the value as a java.math.BigDecimal.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public BigDecimal getBigDecimal(final String columnName) throws SQLException {

        return getBigDecimal(findColumn(columnName));
    }

    /**
     * Returns the value as a java.sql.Date.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public Date getDate(final String columnName) throws SQLException {

        return getDate(findColumn(columnName));
    }

    /**
     * Returns the value as a java.sql.Time.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public Time getTime(final String columnName) throws SQLException {

        return getTime(findColumn(columnName));
    }

    /**
     * Returns the value as a java.sql.Timestamp.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public Timestamp getTimestamp(final String columnName) throws SQLException {

        return getTimestamp(findColumn(columnName));
    }

    /**
     * Returns the value as a java.sql.Array.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     */
    @Override
    public Array getArray(final String columnName) throws SQLException {

        return getArray(findColumn(columnName));
    }

    // ---- result set meta data ---------------------------------------------

    /**
     * Returns the column count.
     * 
     * @return the column count
     */
    @Override
    public int getColumnCount() {

        return columns.size();
    }

    /**
     * Returns 15.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return 15
     */
    @Override
    public int getColumnDisplaySize(final int columnIndex) {

        return 15;
    }

    /**
     * Returns the SQL type.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the SQL type
     */
    @Override
    public int getColumnType(final int columnIndex) throws SQLException {

        return getColumn(columnIndex - 1).sqlType;
    }

    /**
     * Returns the precision.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the precision
     */
    @Override
    public int getPrecision(final int columnIndex) throws SQLException {

        return getColumn(columnIndex - 1).precision;
    }

    /**
     * Returns the scale.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the scale
     */
    @Override
    public int getScale(final int columnIndex) throws SQLException {

        return getColumn(columnIndex - 1).scale;
    }

    /**
     * Returns ResultSetMetaData.columnNullableUnknown.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return columnNullableUnknown
     */
    @Override
    public int isNullable(final int columnIndex) {

        return ResultSetMetaData.columnNullableUnknown;
    }

    /**
     * Returns false.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return false
     */
    @Override
    public boolean isAutoIncrement(final int columnIndex) {

        return false;
    }

    /**
     * Returns true.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return true
     */
    @Override
    public boolean isCaseSensitive(final int columnIndex) {

        return true;
    }

    /**
     * Returns false.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return false
     */
    @Override
    public boolean isCurrency(final int columnIndex) {

        return false;
    }

    /**
     * Returns false.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return false
     */
    @Override
    public boolean isDefinitelyWritable(final int columnIndex) {

        return false;
    }

    /**
     * Returns true.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return true
     */
    @Override
    public boolean isReadOnly(final int columnIndex) {

        return true;
    }

    /**
     * Returns true.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return true
     */
    @Override
    public boolean isSearchable(final int columnIndex) {

        return true;
    }

    /**
     * Returns true.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return true
     */
    @Override
    public boolean isSigned(final int columnIndex) {

        return true;
    }

    /**
     * Returns false.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return false
     */
    @Override
    public boolean isWritable(final int columnIndex) {

        return false;
    }

    /**
     * Returns null.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return null
     */
    @Override
    public String getCatalogName(final int columnIndex) {

        return null;
    }

    /**
     * Returns null.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return null
     */
    @Override
    public String getColumnClassName(final int columnIndex) {

        return null;
    }

    /**
     * Returns the column name.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the column name
     */
    @Override
    public String getColumnLabel(final int columnIndex) throws SQLException {

        return getColumn(columnIndex - 1).name;
    }

    /**
     * Returns the column name.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the column name
     */
    @Override
    public String getColumnName(final int columnIndex) throws SQLException {

        return getColumnLabel(columnIndex);
    }

    /**
     * Returns null.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return null
     */
    @Override
    public String getColumnTypeName(final int columnIndex) {

        return null;
    }

    /**
     * Returns null.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return null
     */
    @Override
    public String getSchemaName(final int columnIndex) {

        return null;
    }

    /**
     * Returns null.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return null
     */
    @Override
    public String getTableName(final int columnIndex) {

        return null;
    }

    // ---- unsupported / result set
    // ---------------------------------------------

    /**
     * INTERNAL
     */
    @Override
    public void clearWarnings() {

        // nothing to do
    }

    /**
     * INTERNAL
     */
    @Override
    public void afterLast() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void cancelRowUpdates() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateNull(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void deleteRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void insertRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void moveToCurrentRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void moveToInsertRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void refreshRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateRow() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean first() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean isAfterLast() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean isBeforeFirst() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean isFirst() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean isLast() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean last() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean previous() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean rowDeleted() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean rowInserted() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean rowUpdated() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void setFetchDirection(final int direction) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void setFetchSize(final int rows) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateNull(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean absolute(final int row) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public boolean relative(final int rows) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateLong(final int columnIndex, final long x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public InputStream getAsciiStream(final int columnIndex) {

        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public InputStream getBinaryStream(final int columnIndex) {

        return null;
    }

    /**
     * @deprecated INTERNAL
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(final int columnIndex) {

        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateObject(final int columnIndex, final Object x, final int scale) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public String getCursorName() throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateByte(final String columnName, final byte x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateDouble(final String columnName, final double x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateFloat(final String columnName, final float x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateInt(final String columnName, final int x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateLong(final String columnName, final long x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateShort(final String columnName, final short x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBoolean(final String columnName, final boolean x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBytes(final String columnName, final byte[] x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * @deprecated INTERNAL
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public URL getURL(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Blob getBlob(final int i) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Clob getClob(final int i) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Ref getRef(final int i) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public InputStream getAsciiStream(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public InputStream getBinaryStream(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * @deprecated INTERNAL
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateAsciiStream(final String columnName, final InputStream x, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBinaryStream(final String columnName, final InputStream x, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Reader getCharacterStream(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateCharacterStream(final String columnName, final Reader reader, final int length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateObject(final String columnName, final Object x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateObject(final String columnName, final Object x, final int scale) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getObject(final int i, final Map map) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateString(final String columnName, final String x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * @deprecated INTERNAL
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final String columnName, final int scale) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBigDecimal(final String columnName, final BigDecimal x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public URL getURL(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateArray(final String columnName, final Array x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Blob getBlob(final String colName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateBlob(final String columnName, final Blob x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Clob getClob(final String colName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateClob(final String columnName, final Clob x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateDate(final String columnName, final Date x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Ref getRef(final String colName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateRef(final String columnName, final Ref x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateTime(final String columnName, final Time x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public void updateTimestamp(final String columnName, final Timestamp x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Object getObject(final String colName, final Map map) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Date getDate(final String columnName, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Time getTime(final String columnName, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public Timestamp getTimestamp(final String columnName, final Calendar cal) throws SQLException {

        throw getUnsupportedException();
    }

    // --- private -----------------------------

    static SQLException getUnsupportedException() {

        return new SQLException("Feature not supported", "HYC00");
    }

    private void checkColumnIndex(final int columnIndex) throws SQLException {

        if (columnIndex < 0 || columnIndex >= columns.size()) { throw new SQLException("Invalid column index " + (columnIndex + 1), "90009"); }
    }

    private Object get(int columnIndex) throws SQLException {

        if (currentRow == null) { throw new SQLException("No data is available", "02000"); }
        columnIndex--;
        checkColumnIndex(columnIndex);
        final Object o = columnIndex < currentRow.length ? currentRow[columnIndex] : null;
        wasNull = o == null;
        return o;
    }

    private Column getColumn(final int i) throws SQLException {

        checkColumnIndex(i);
        return (Column) columns.get(i);
    }

    /**
     * INTERNAL
     */

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public RowId getRowId(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateRowId(final String columnName, final RowId x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * Returns the current result set holdability.
     * 
     * @return the holdability
     */
    // ## Java 1.4 begin ##
    @Override
    public int getHoldability() {

        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    // ## Java 1.4 end ##

    /**
     * Returns whether this result set has been closed.
     * 
     * @return true if the result set was closed
     */
    @Override
    public boolean isClosed() {

        return rows == null;
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNString(final int columnIndex, final String nString) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNString(final String columnName, final String nString) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final int columnIndex, final NClob nClob) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final String columnName, final NClob nClob) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public NClob getNClob(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public SQLXML getSQLXML(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateSQLXML(final String columnName, final SQLXML xmlObject) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public String getNString(final int columnIndex) throws SQLException {

        return getString(columnIndex);
    }

    /**
     * INTERNAL
     */
    @Override
    public String getNString(final String columnName) throws SQLException {

        return getString(columnName);
    }

    /**
     * INTERNAL
     */

    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public Reader getNCharacterStream(final String columnName) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateAsciiStream(final String columnName, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateAsciiStream(final String columnName, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBinaryStream(final int columnName, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBinaryStream(final String columnName, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBinaryStream(final String columnName, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBlob(final int columnIndex, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBlob(final String columnName, final InputStream x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBlob(final int columnIndex, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateBlob(final String columnName, final InputStream x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateCharacterStream(final String columnName, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateCharacterStream(final String columnName, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateClob(final int columnIndex, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateClob(final String columnName, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateClob(final int columnIndex, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateClob(final String columnName, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNCharacterStream(final String columnName, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNCharacterStream(final String columnName, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final int columnIndex, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final String columnName, final Reader x) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final int columnIndex, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    /**
     * INTERNAL
     */

    @Override
    public void updateNClob(final String columnName, final Reader x, final long length) throws SQLException {

        throw getUnsupportedException();
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {

        throw Message.getUnsupportedException();
    }

}
