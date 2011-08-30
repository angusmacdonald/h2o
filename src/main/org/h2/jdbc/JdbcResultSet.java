/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.jdbc;

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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.result.UpdatableRow;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * <p>
 * Represents a result set.
 * </p>
 * <p>
 * Column names are case-insensitive, quotes are not supported. The first column has the column index 1.
 * </p>
 * <p>
 * Updatable result sets: Result sets are updatable when the result only contains columns from one table, and if it contains all columns of
 * a unique index (primary key or other) of this table. Key columns may not contain NULL (because multiple rows with NULL could exist). In
 * updatable result sets, own changes are visible, but not own inserts and deletes.
 * </p>
 */
public class JdbcResultSet extends TraceObject implements ResultSet {

    private final boolean closeStatement;

    private final boolean scrollable;

    private ResultInterface result;

    private JdbcConnection conn;

    private JdbcStatement stat;

    private int columnCount;

    private boolean wasNull;

    private Value[] insertRow;

    private Value[] updateRow;

    private HashMap columnNameMap;

    private HashMap patchedRows;

    JdbcResultSet(final JdbcConnection conn, final JdbcStatement stat, final ResultInterface result, final int id, final boolean closeStatement, final boolean scrollable) {

        setTrace(conn.getSession().getTrace(), TraceObject.RESULT_SET, id);
        this.conn = conn;
        this.stat = stat;
        this.result = result;
        columnCount = result.getVisibleColumnCount();
        this.closeStatement = closeStatement;
        this.scrollable = scrollable;
    }

    /**
     * Moves the cursor to the next row of the result set.
     * 
     * @return true if successful, false if there are no more rows
     */
    @Override
    public boolean next() throws SQLException {

        try {
            debugCodeCall("next");
            checkClosed();
            return nextRow();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the meta data of this result set.
     * 
     * @return the meta data
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {

        try {
            final int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            final String catalog = conn.getCatalog();
            final JdbcResultSetMetaData meta = new JdbcResultSetMetaData(this, null, result, catalog, conn.getSession().getTrace(), id);
            return meta;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether the last column accessed was a null value.
     * 
     * @return true if the last column accessed was a null value
     */
    @Override
    public boolean wasNull() throws SQLException {

        try {
            debugCodeCall("wasNull");
            checkClosed();
            return wasNull;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
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

        try {
            debugCodeCall("findColumn", columnName);
            return getColumnIndex(columnName);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes the result set.
     */
    @Override
    public void close() throws SQLException {

        try {
            debugCodeCall("close");
            closeInternal();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Close the result set. This method also closes the statement if required.
     */
    void closeInternal() throws SQLException {

        if (result != null) {
            try {
                result.close();
                if (closeStatement && stat != null) {
                    stat.close();
                }
            }
            finally {
                columnCount = 0;
                result = null;
                stat = null;
                conn = null;
                insertRow = null;
                updateRow = null;
            }
        }
    }

    /**
     * Returns the statement that created this object.
     * 
     * @return the statement or prepared statement, or null if created by a DatabaseMetaData call.
     */
    @Override
    public Statement getStatement() throws SQLException {

        try {
            debugCodeCall("getStatement");
            checkClosed();
            if (closeStatement) {
                // if the result set was opened by a DatabaseMetaData call
                return null;
            }
            return stat;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the first warning reported by calls on this object.
     * 
     * @return null
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {

        try {
            debugCodeCall("getWarnings");
            checkClosed();
            return null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all warnings.
     */
    @Override
    public void clearWarnings() throws SQLException {

        try {
            debugCodeCall("clearWarnings");
            checkClosed();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public String getString(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getString", columnIndex);
            return get(columnIndex).getString();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public String getString(final String columnName) throws SQLException {

        try {
            debugCodeCall("getString", columnName);
            return get(columnName).getString();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public int getInt(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getInt", columnIndex);
            return get(columnIndex).getInt();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an int.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public int getInt(final String columnName) throws SQLException {

        try {
            debugCodeCall("getInt", columnName);
            return get(columnName).getInt();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getBigDecimal", columnIndex);
            return get(columnIndex).getBigDecimal();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Date getDate(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getDate", columnIndex);
            return get(columnIndex).getDate();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Time getTime(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getTime", columnIndex);
            return get(columnIndex).getTime();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getTimestamp", columnIndex);
            return get(columnIndex).getTimestamp();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public BigDecimal getBigDecimal(final String columnName) throws SQLException {

        try {
            debugCodeCall("getBigDecimal", columnName);
            return get(columnName).getBigDecimal();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Date getDate(final String columnName) throws SQLException {

        try {
            debugCodeCall("getDate", columnName);
            return get(columnName).getDate();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Time getTime(final String columnName) throws SQLException {

        try {
            debugCodeCall("getTime", columnName);
            return get(columnName).getTime();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Timestamp getTimestamp(final String columnName) throws SQLException {

        try {
            debugCodeCall("getTimestamp", columnName);
            return get(columnName).getTimestamp();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is de-serialized into a Java Object.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value or null
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Object getObject(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getObject", columnIndex);
            final Value v = get(columnIndex);
            return conn.convertToDefaultObject(v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns a column value as a Java object. For BINARY data, the data is de-serialized into a Java Object.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value or null
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Object getObject(final String columnName) throws SQLException {

        try {
            debugCodeCall("getObject", columnName);
            final Value v = get(columnName);
            return conn.convertToDefaultObject(v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getBoolean", columnIndex);
            final Boolean v = get(columnIndex).getBoolean();
            return v == null ? false : v.booleanValue();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a boolean.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public boolean getBoolean(final String columnName) throws SQLException {

        try {
            debugCodeCall("getBoolean", columnName);
            final Boolean v = get(columnName).getBoolean();
            return v == null ? false : v.booleanValue();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public byte getByte(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getByte", columnIndex);
            return get(columnIndex).getByte();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public byte getByte(final String columnName) throws SQLException {

        try {
            debugCodeCall("getByte", columnName);
            return get(columnName).getByte();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public short getShort(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getShort", columnIndex);
            return get(columnIndex).getShort();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a short.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public short getShort(final String columnName) throws SQLException {

        try {
            debugCodeCall("getShort", columnName);
            return get(columnName).getShort();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public long getLong(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getLong", columnIndex);
            return get(columnIndex).getLong();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a long.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public long getLong(final String columnName) throws SQLException {

        try {
            debugCodeCall("getLong", columnName);
            return get(columnName).getLong();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public float getFloat(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getFloat", columnIndex);
            return get(columnIndex).getFloat();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a float.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public float getFloat(final String columnName) throws SQLException {

        try {
            debugCodeCall("getFloat", columnName);
            return get(columnName).getFloat();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public double getDouble(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getDouble", columnIndex);
            return get(columnIndex).getDouble();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a double.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public double getDouble(final String columnName) throws SQLException {

        try {
            debugCodeCall("getDouble", columnName);
            return get(columnName).getDouble();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @deprecated
     * 
     * @param columnName
     *            the column name
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final String columnName, final int scale) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + StringUtils.quoteJavaString(columnName) + ", " + scale + ");");
            }
            if (scale < 0) { throw Message.getInvalidValueException("" + scale, "scale"); }
            final BigDecimal bd = get(columnName).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @deprecated
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getBigDecimal(" + columnIndex + ", " + scale + ");");
            }
            if (scale < 0) { throw Message.getInvalidValueException("" + scale, "scale"); }
            final BigDecimal bd = get(columnIndex).getBigDecimal();
            return bd == null ? null : MathUtils.setScale(bd, scale);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * 
     * @deprecated
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getUnicodeStream", columnIndex);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     * 
     * @deprecated
     */
    @Deprecated
    @Override
    public InputStream getUnicodeStream(final String columnName) throws SQLException {

        try {
            debugCodeCall("getUnicodeStream", columnName);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a object using the specified type mapping.
     */
    @Override
    public Object getObject(final int columnIndex, final Map map) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getObject(" + columnIndex + ", map);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a object using the specified type mapping.
     */
    @Override
    public Object getObject(final String columnName, final Map map) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getObject(" + quote(columnName) + ", map);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getRef", columnIndex);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets a column as a reference.
     */
    @Override
    public Ref getRef(final String columnName) throws SQLException {

        try {
            debugCodeCall("getRef", columnName);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a specified time zone.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Date getDate(final int columnIndex, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + columnIndex + ", calendar)");
            }
            final Date x = get(columnIndex).getDate();
            return DateTimeUtils.convertDateToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Date using a specified time zone.
     * 
     * @param columnName
     *            the name of the column label
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Date getDate(final String columnName, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getDate(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            final Date x = get(columnName).getDate();
            return DateTimeUtils.convertDateToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a specified time zone.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Time getTime(final int columnIndex, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + columnIndex + ", calendar)");
            }
            final Time x = get(columnIndex).getTime();
            return DateTimeUtils.convertTimeToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Time using a specified time zone.
     * 
     * @param columnName
     *            the name of the column label
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Time getTime(final String columnName, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getTime(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            final Time x = get(columnName).getTime();
            return DateTimeUtils.convertTimeToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp using a specified time zone.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + columnIndex + ", calendar)");
            }
            final Timestamp x = get(columnIndex).getTimestamp();
            return DateTimeUtils.convertTimestampToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a java.sql.Timestamp.
     * 
     * @param columnName
     *            the name of the column label
     * @param calendar
     *            the calendar
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Timestamp getTimestamp(final String columnName, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("getTimestamp(" + StringUtils.quoteJavaString(columnName) + ", calendar)");
            }
            final Timestamp x = get(columnName).getTimestamp();
            return DateTimeUtils.convertTimestampToCalendar(x, calendar);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {

        try {
            final int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + columnIndex + ")");
            final Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Blob.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Blob getBlob(final String columnName) throws SQLException {

        try {
            final int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "getBlob(" + quote(columnName) + ")");
            final Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcBlob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getBytes", columnIndex);
            return get(columnIndex).getBytes();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a byte array.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public byte[] getBytes(final String columnName) throws SQLException {

        try {
            debugCodeCall("getBytes", columnName);
            return get(columnName).getBytes();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getBinaryStream", columnIndex);
            return get(columnIndex).getInputStream();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public InputStream getBinaryStream(final String columnName) throws SQLException {

        try {
            debugCodeCall("getBinaryStream", columnName);
            return get(columnName).getInputStream();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Clob getClob(final int columnIndex) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + columnIndex + ")");
            final Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Clob getClob(final String columnName) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "getClob(" + quote(columnName) + ")");
            final Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Array getArray(final int columnIndex) throws SQLException {

        try {
            final int id = getNextId(TraceObject.ARRAY);
            debugCodeAssign("Clob", TraceObject.ARRAY, id, "getArray(" + columnIndex + ")");
            final Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as an Array.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Array getArray(final String columnName) throws SQLException {

        try {
            final int id = getNextId(TraceObject.ARRAY);
            debugCodeAssign("Clob", TraceObject.ARRAY, id, "getArray(" + quote(columnName) + ")");
            final Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcArray(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getAsciiStream", columnIndex);
            final String s = get(columnIndex).getString();
            return s == null ? null : IOUtils.getInputStream(s);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public InputStream getAsciiStream(final String columnName) throws SQLException {

        try {
            debugCodeCall("getAsciiStream", columnName);
            final String s = get(columnName).getString();
            return IOUtils.getInputStream(s);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Reader getCharacterStream(final String columnName) throws SQLException {

        try {
            debugCodeCall("getCharacterStream", columnName);
            return get(columnName).getReader();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getURL", columnIndex);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(final String columnName) throws SQLException {

        try {
            debugCodeCall("getURL", columnName);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNull(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("updateNull", columnIndex);
            update(columnIndex, ValueNull.INSTANCE);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNull(final String columnName) throws SQLException {

        try {
            debugCodeCall("updateNull", columnName);
            update(columnName, ValueNull.INSTANCE);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean(" + columnIndex + ", " + x + ");");
            }
            update(columnIndex, ValueBoolean.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if result set is closed
     */
    @Override
    public void updateBoolean(final String columnName, final boolean x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBoolean(" + quote(columnName) + ", " + x + ");");
            }
            update(columnName, ValueBoolean.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateByte(" + columnIndex + ", " + x + ");");
            }
            update(columnIndex, ValueByte.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateByte(final String columnName, final byte x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateByte(" + columnName + ", " + x + ");");
            }
            update(columnName, ValueByte.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes(" + columnIndex + ", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBytes(final String columnName, final byte[] x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBytes(" + quote(columnName) + ", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateShort(" + columnIndex + ", (short) " + x + ");");
            }
            update(columnIndex, ValueShort.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateShort(final String columnName, final short x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateShort(" + quote(columnName) + ", (short) " + x + ");");
            }
            update(columnName, ValueShort.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateInt(" + columnIndex + ", " + x + ");");
            }
            update(columnIndex, ValueInt.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateInt(final String columnName, final int x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateInt(" + quote(columnName) + ", " + x + ");");
            }
            update(columnName, ValueInt.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateLong(final int columnIndex, final long x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateLong(" + columnIndex + ", " + x + "L);");
            }
            update(columnIndex, ValueLong.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateLong(final String columnName, final long x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateLong(" + quote(columnName) + ", " + x + "L);");
            }
            update(columnName, ValueLong.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat(" + columnIndex + ", " + x + "f);");
            }
            update(columnIndex, ValueFloat.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateFloat(final String columnName, final float x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateFloat(" + quote(columnName) + ", " + x + "f);");
            }
            update(columnName, ValueFloat.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble(" + columnIndex + ", " + x + "d);");
            }
            update(columnIndex, ValueDouble.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateDouble(final String columnName, final double x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateDouble(" + quote(columnName) + ", " + x + "d);");
            }
            update(columnName, ValueDouble.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal(" + columnIndex + ", " + quoteBigDecimal(x) + ");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBigDecimal(final String columnName, final BigDecimal x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBigDecimal(" + quote(columnName) + ", " + quoteBigDecimal(x) + ");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateString(" + columnIndex + ", " + quote(x) + ");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateString(final String columnName, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateString(" + quote(columnName) + ", " + quote(x) + ");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateDate(" + columnIndex + ", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateDate(final String columnName, final Date x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateDate(" + quote(columnName) + ", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateTime(" + columnIndex + ", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateTime(final String columnName, final Time x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateTime(" + quote(columnName) + ", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp(" + columnIndex + ", x);");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateTimestamp(final String columnName, final Timestamp x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateTimestamp(" + quote(columnName) + ", x);");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {

        updateAsciiStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {

        updateAsciiStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final String columnName, final InputStream x, final int length) throws SQLException {

        updateAsciiStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final String columnName, final InputStream x) throws SQLException {

        updateAsciiStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateAsciiStream(final String columnName, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateAsciiStream(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {

        updateBinaryStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {

        updateBinaryStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final String columnName, final InputStream x) throws SQLException {

        updateBinaryStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final String columnName, final InputStream x, final int length) throws SQLException {

        updateBinaryStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBinaryStream(final String columnName, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBinaryStream(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, length);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {

        updateCharacterStream(columnIndex, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {

        updateCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final String columnName, final Reader x, final int length) throws SQLException {

        updateCharacterStream(columnName, x, (long) length);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final String columnName, final Reader x) throws SQLException {

        updateCharacterStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateCharacterStream(final String columnName, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateCharacterStream(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param scale
     *            is ignored
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateObject(final int columnIndex, final Object x, final int scale) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x, " + scale + ");");
            }
            update(columnIndex, convertToUnknownValue(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param scale
     *            is ignored
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateObject(final String columnName, final Object x, final int scale) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnName) + ", x, " + scale + ");");
            }
            update(columnName, convertToUnknownValue(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + columnIndex + ", x);");
            }
            update(columnIndex, convertToUnknownValue(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateObject(final String columnName, final Object x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateObject(" + quote(columnName) + ", x);");
            }
            update(columnName, convertToUnknownValue(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateRef(" + columnIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateRef(final String columnName, final Ref x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateRef(" + quote(columnName) + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final int columnIndex, final InputStream x) throws SQLException {

        updateBlob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the length
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final int columnIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + columnIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final String columnName, final Blob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + quote(columnName) + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final String columnName, final InputStream x) throws SQLException {

        updateBlob(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the length
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateBlob(final String columnName, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateBlob(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, -1);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + columnIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final int columnIndex, final Reader x) throws SQLException {

        updateClob(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the length
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final int columnIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final String columnName, final Clob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + quote(columnName) + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final String columnName, final Reader x) throws SQLException {

        updateClob(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the length
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateClob(final String columnName, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateClob(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateArray(" + columnIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void updateArray(final String columnName, final Array x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateArray(" + quote(columnName) + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Gets the cursor name if it was defined. This feature is superseded by updateX methods. This method throws a
     * SQLException because cursor names are not supported.
     */
    @Override
    public String getCursorName() throws SQLException {

        try {
            debugCodeCall("getCursorName");
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current row number. The first row is row 1, the second 2 and so on. This method returns 0 before the first and after the
     * last row.
     * 
     * @return the row number
     */
    @Override
    public int getRow() throws SQLException {

        try {
            debugCodeCall("getRow");
            checkClosed();
            final int rowId = result.getRowId();
            if (rowId >= result.getRowCount()) { return 0; }
            return rowId + 1;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set concurrency.
     * 
     * @return ResultSet.CONCUR_UPDATABLE
     */
    @Override
    public int getConcurrency() throws SQLException {

        try {
            debugCodeCall("getConcurrency");
            checkClosed();
            final UpdatableRow row = new UpdatableRow(conn, result);
            return row.isUpdatable() ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the fetch direction.
     * 
     * @return the direction: FETCH_FORWARD
     */
    @Override
    public int getFetchDirection() throws SQLException {

        try {
            debugCodeCall("getFetchDirection");
            checkClosed();
            return ResultSet.FETCH_FORWARD;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the number of rows suggested to read in one step.
     * 
     * @return the current fetch size
     */
    @Override
    public int getFetchSize() throws SQLException {

        try {
            debugCodeCall("getFetchSize");
            checkClosed();
            return result.getFetchSize();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the number of rows suggested to read in one step. This value cannot be higher than the maximum rows (setMaxRows) set by the
     * statement or prepared statement, otherwise an exception is throws. Setting the value to 0 will set the default value. The default
     * value can be changed using the system property h2.serverResultSetFetchSize.
     * 
     * @param rows
     *            the number of rows
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {

        try {
            debugCodeCall("setFetchSize", rows);
            checkClosed();

            if (rows < 0) {
                throw Message.getInvalidValueException("" + rows, "rows");
            }
            else if (rows > 0) {
                if (stat != null) {
                    final int maxRows = stat.getMaxRows();
                    if (maxRows > 0 && rows > maxRows) { throw Message.getInvalidValueException("" + rows, "rows"); }
                }
            }
            else {
                rows = SysProperties.SERVER_RESULT_SET_FETCH_SIZE;
            }
            result.setFetchSize(rows);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets (changes) the fetch direction for this result set. This method should only be called for scrollable result sets, otherwise it
     * will throw an exception (no matter what direction is used).
     * 
     * @param direction
     *            the new fetch direction
     * @throws SQLException
     *             Unsupported Feature if the method is called for a forward-only result set
     */
    @Override
    public void setFetchDirection(final int direction) throws SQLException {

        try {
            debugCodeCall("setFetchDirection", direction);
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the result set type.
     * 
     * @return the result set type (TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE or TYPE_SCROLL_SENSITIVE)
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public int getType() throws SQLException {

        try {
            debugCodeCall("getType");
            checkClosed();
            return stat == null ? ResultSet.TYPE_FORWARD_ONLY : stat.resultSetType;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is before the first row, that means next() was not called yet.
     * 
     * @return if the current position is before the first row
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean isBeforeFirst() throws SQLException {

        try {
            debugCodeCall("isBeforeFirst");
            checkClosed();
            return result.getRowId() < 0;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is after the last row, that means next() was called and returned false.
     * 
     * @return if the current position is after the last row
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean isAfterLast() throws SQLException {

        try {
            debugCodeCall("isAfterLast");
            checkClosed();
            final int row = result.getRowId();
            final int count = result.getRowCount();
            return row >= count || count == 0;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is row 1, that means next() was called once and returned true.
     * 
     * @return if the current position is the first row
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean isFirst() throws SQLException {

        try {
            debugCodeCall("isFirst");
            checkClosed();
            final int row = result.getRowId();
            return row == 0 && row < result.getRowCount();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if the current position is the last row, that means next() was called and did not yet returned false, but will in the next
     * call.
     * 
     * @return if the current position is the last row
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean isLast() throws SQLException {

        try {
            debugCodeCall("isLast");
            checkClosed();
            final int row = result.getRowId();
            return row >= 0 && row == result.getRowCount() - 1;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to before the first row, that means resets the result set.
     * 
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void beforeFirst() throws SQLException {

        try {
            debugCodeCall("beforeFirst");
            checkClosed();
            if (result.getRowId() >= 0) {
                resetResult();
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to after the last row, that means after the end.
     * 
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void afterLast() throws SQLException {

        try {
            debugCodeCall("afterLast");
            checkClosed();
            while (nextRow()) {
                // nothing
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the first row. This is the same as calling beforeFirst() followed by next().
     * 
     * @return true if there is a row available, false if not
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean first() throws SQLException {

        try {
            debugCodeCall("first");
            checkClosed();
            if (result.getRowId() < 0) { return nextRow(); }
            resetResult();
            return nextRow();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the last row.
     * 
     * @return true if there is a row available, false if not
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean last() throws SQLException {

        try {
            debugCodeCall("last");
            checkClosed();
            return absolute(-1);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row.
     * 
     * @param rowNumber
     *            the row number. 0 is not allowed, 1 means the first row, 2 the second. -1 means the last row, -2 the row before the last
     *            row. If the value is too large, the position is moved after the last row, if if the value is too small it is moved before
     *            the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean absolute(int rowNumber) throws SQLException {

        try {
            debugCodeCall("absolute", rowNumber);
            checkClosed();
            if (rowNumber < 0) {
                rowNumber = result.getRowCount() + rowNumber + 1;
            }
            else if (rowNumber > result.getRowCount() + 1) {
                rowNumber = result.getRowCount() + 1;
            }
            if (rowNumber <= result.getRowId()) {
                resetResult();
            }
            while (result.getRowId() + 1 < rowNumber) {
                nextRow();
            }
            final int row = result.getRowId();
            return row >= 0 && row < result.getRowCount();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to a specific row relative to the current row.
     * 
     * @param rowCount
     *            0 means don't do anything, 1 is the next row, -1 the previous. If the value is too large, the position is moved after the
     *            last row, if if the value is too small it is moved before the first row.
     * @return true if there is a row available, false if not
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean relative(final int rowCount) throws SQLException {

        try {
            debugCodeCall("relative", rowCount);
            checkClosed();
            int row = result.getRowId() + 1 + rowCount;
            if (row < 0) {
                row = 0;
            }
            else if (row > result.getRowCount()) {
                row = result.getRowCount() + 1;
            }
            return absolute(row);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the cursor to the last row, or row before first row if the current position is the first row.
     * 
     * @return true if there is a row available, false if not
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public boolean previous() throws SQLException {

        try {
            debugCodeCall("previous");
            checkClosed();
            return relative(-1);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the insert row. The current row is remembered.
     * 
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void moveToInsertRow() throws SQLException {

        try {
            debugCodeCall("moveToInsertRow");
            checkClosed();
            insertRow = new Value[columnCount];
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Moves the current position to the current row.
     * 
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void moveToCurrentRow() throws SQLException {

        try {
            debugCodeCall("moveToCurrentRow");
            checkClosed();
            insertRow = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was updated (by somebody else or the caller).
     * 
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowUpdated() throws SQLException {

        try {
            debugCodeCall("rowUpdated");
            return false;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was inserted.
     * 
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowInserted() throws SQLException {

        try {
            debugCodeCall("rowInserted");
            return false;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Detects if the row was deleted (by somebody else or the caller).
     * 
     * @return false because this driver does not detect this
     */
    @Override
    public boolean rowDeleted() throws SQLException {

        try {
            debugCodeCall("rowDeleted");
            return false;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Inserts the current row. The current position must be the insert row.
     * 
     * @throws SQLException
     *             if the result set is closed or if not on the insert row
     */
    @Override
    public void insertRow() throws SQLException {

        try {
            debugCodeCall("insertRow");
            checkClosed();
            if (insertRow == null) { throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW); }
            getUpdatableRow().insertRow(insertRow);
            insertRow = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates the current row.
     * 
     * @throws SQLException
     *             if the result set is closed or if the current row is the insert row or if not on a valid row
     */
    @Override
    public void updateRow() throws SQLException {

        try {
            debugCodeCall("updateRow");
            checkClosed();
            if (insertRow != null) { throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW); }
            checkOnValidRow();
            if (updateRow != null) {
                final UpdatableRow row = getUpdatableRow();
                final Value[] current = new Value[columnCount];
                for (int i = 0; i < updateRow.length; i++) {
                    current[i] = get(i + 1);
                }
                row.updateRow(current, updateRow);
                for (int i = 0; i < updateRow.length; i++) {
                    if (updateRow[i] == null) {
                        updateRow[i] = current[i];
                    }
                }
                final Value[] patch = row.readRow(updateRow);
                patchCurrentRow(patch);
                updateRow = null;
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Deletes the current row.
     * 
     * @throws SQLException
     *             if the result set is closed or if the current row is the insert row or if not on a valid row
     */
    @Override
    public void deleteRow() throws SQLException {

        try {
            debugCodeCall("deleteRow");
            checkClosed();
            if (insertRow != null) { throw Message.getSQLException(ErrorCode.NOT_ON_UPDATABLE_ROW); }
            checkOnValidRow();
            getUpdatableRow().deleteRow(result.currentRow());
            updateRow = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Re-reads the current row from the database.
     * 
     * @throws SQLException
     *             if the result set is closed or if the current row is the insert row or if the row has been deleted or if not on a valid
     *             row
     */
    @Override
    public void refreshRow() throws SQLException {

        try {
            debugCodeCall("refreshRow");
            checkClosed();
            if (insertRow != null) { throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE); }
            checkOnValidRow();
            patchCurrentRow(getUpdatableRow().readRow(result.currentRow()));
            updateRow = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Cancels updating a row.
     * 
     * @throws SQLException
     *             if the result set is closed or if the current row is the insert row
     */
    @Override
    public void cancelRowUpdates() throws SQLException {

        try {
            debugCodeCall("cancelRowUpdates");
            checkClosed();
            if (insertRow != null) { throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE); }
            updateRow = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private UpdatableRow getUpdatableRow() throws SQLException {

        final UpdatableRow row = new UpdatableRow(conn, result);
        if (!row.isUpdatable()) { throw Message.getSQLException(ErrorCode.RESULT_SET_NOT_UPDATABLE); }
        return row;
    }

    private int getColumnIndex(final String columnName) throws SQLException {

        checkClosed();
        if (columnName == null) { throw Message.getInvalidValueException("columnName", null); }
        if (columnCount >= SysProperties.MIN_COLUMN_NAME_MAP) {
            if (columnNameMap == null) {
                final HashMap map = new HashMap(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    String c = result.getAlias(i).toUpperCase();
                    map.put(c, ObjectUtils.getInteger(i));
                    final String tabName = result.getTableName(i);
                    if (tabName != null) {
                        final String colName = result.getColumnName(i);
                        if (colName != null) {
                            c = tabName + "." + colName;
                            if (!map.containsKey(c)) {
                                map.put(c, ObjectUtils.getInteger(i));
                            }
                        }
                    }
                }
                columnNameMap = map;
            }
            final Integer index = (Integer) columnNameMap.get(columnName.toUpperCase());
            if (index == null) { throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName); }
            return index.intValue() + 1;
        }
        for (int i = 0; i < columnCount; i++) {
            if (columnName.equalsIgnoreCase(result.getAlias(i))) { return i + 1; }
        }
        final int idx = columnName.indexOf('.');
        if (idx > 0) {
            final String table = columnName.substring(0, idx);
            final String col = columnName.substring(idx + 1);
            for (int i = 0; i < columnCount; i++) {
                if (table.equalsIgnoreCase(result.getTableName(i)) && col.equalsIgnoreCase(result.getColumnName(i))) { return i + 1; }
            }
        }
        throw Message.getSQLException(ErrorCode.COLUMN_NOT_FOUND_1, columnName);
    }

    private void checkColumnIndex(final int columnIndex) throws SQLException {

        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) { throw Message.getInvalidValueException("" + columnIndex, "columnIndex"); }
    }

    /**
     * Check if this result set is closed.
     * 
     * @throws SQLException
     *             if it is closed
     */
    void checkClosed() throws SQLException {

        if (result == null) { throw Message.getSQLException(ErrorCode.OBJECT_CLOSED); }
        if (stat != null) {
            stat.checkClosed();
        }
        if (conn != null) {
            conn.checkClosed();
        }
    }

    private void checkOnValidRow() throws SQLException {

        if (result.getRowId() < 0 || result.getRowId() >= result.getRowCount()) { throw Message.getSQLException(ErrorCode.NO_DATA_AVAILABLE); }
    }

    private Value get(final int columnIndex) throws SQLException {

        checkColumnIndex(columnIndex);
        checkOnValidRow();
        Value[] list;
        if (patchedRows == null) {
            list = result.currentRow();
        }
        else {
            list = (Value[]) patchedRows.get(ObjectUtils.getInteger(result.getRowId()));
            if (list == null) {
                list = result.currentRow();
            }
        }

        // create table australia (ID INTEGER NOT NULL, Name VARCHAR(100),
        // FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY
        // KEY(ID))
        final Value value = list[columnIndex - 1];
        wasNull = value == ValueNull.INSTANCE;
        return value;
    }

    private Value get(final String columnName) throws SQLException {

        final int columnIndex = getColumnIndex(columnName);
        return get(columnIndex);
    }

    private void update(final String columnName, final Value v) throws SQLException {

        final int columnIndex = getColumnIndex(columnName);
        update(columnIndex, v);
    }

    private void update(final int columnIndex, final Value v) throws SQLException {

        checkColumnIndex(columnIndex);
        if (insertRow != null) {
            insertRow[columnIndex - 1] = v;
        }
        else {
            if (updateRow == null) {
                updateRow = new Value[columnCount];
            }
            updateRow[columnIndex - 1] = v;
        }
    }

    private boolean nextRow() throws SQLException {

        final boolean next = result.next();
        if (!next && !scrollable) {
            result.close();
        }
        return next;
    }

    private void resetResult() throws SQLException {

        if (!scrollable) { throw Message.getSQLException(ErrorCode.RESULT_SET_NOT_SCROLLABLE); }
        result.reset();
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     * 
     * @param columnIndex
     *            (1,2,...)
     */

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Returns the value of the specified column as a row id.
     * 
     * @param columnName
     *            the name of the column label
     */

    @Override
    public RowId getRowId(final String columnName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     */

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     */

    @Override
    public void updateRowId(final String columnName, final RowId x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Returns the current result set holdability.
     * 
     * @return the holdability
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public int getHoldability() throws SQLException {

        try {
            debugCodeCall("getHoldability");
            checkClosed();
            return conn.getHoldability();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns whether this result set is closed.
     * 
     * @return true if the result set is closed
     */
    @Override
    public boolean isClosed() throws SQLException {

        try {
            debugCodeCall("isClosed");
            return result == null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNString(final int columnIndex, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNString(" + columnIndex + ", " + quote(x) + ");");
            }
            update(columnIndex, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNString(final String columnName, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNString(" + quote(columnName) + ", " + quote(x) + ");");
            }
            update(columnName, x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final int columnIndex, final NClob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final int columnIndex, final Reader x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final int columnIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + columnIndex + ", x, " + length + "L);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final String columnName, final Reader x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnName) + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final String columnName, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnName) + ", x, " + length + "L);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */

    @Override
    public void updateNClob(final String columnName, final NClob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNClob(" + quote(columnName) + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnIndex + ")");
            final Value v = get(columnIndex);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a Clob.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */

    @Override
    public NClob getNClob(final String columnName) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "getNClob(" + columnName + ")");
            final Value v = get(columnName);
            return v == ValueNull.INSTANCE ? null : new JdbcClob(conn, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Returns the value of the specified column as a SQLXML object.
     */

    @Override
    public SQLXML getSQLXML(final String columnName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     */

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Updates a column in the current or insert row.
     */

    @Override
    public void updateSQLXML(final String columnName, final SQLXML xmlObject) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public String getNString(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getNString", columnIndex);
            return get(columnIndex).getString();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as a String.
     * 
     * @param columnName
     *            the column name
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public String getNString(final String columnName) throws SQLException {

        try {
            debugCodeCall("getNString", columnName);
            return get(columnName).getString();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Reader getNCharacterStream(final int columnIndex) throws SQLException {

        try {
            debugCodeCall("getNCharacterStream", columnIndex);
            return get(columnIndex).getReader();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the value of the specified column as input stream.
     * 
     * @param columnName
     *            the name of the column label
     * @return the value
     * @throws SQLException
     *             if the column is not found or if the result set is closed
     */
    @Override
    public Reader getNCharacterStream(final String columnName) throws SQLException {

        try {
            debugCodeCall("getNCharacterStream", columnName);
            return get(columnName).getReader();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {

        updateNCharacterStream(columnIndex, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnIndex
     *            (1,2,...)
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + columnIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNCharacterStream(final String columnName, final Reader x) throws SQLException {

        updateNCharacterStream(columnName, x, -1);
    }

    /**
     * Updates a column in the current or insert row.
     * 
     * @param columnName
     *            the name of the column label
     * @param x
     *            the value
     * @param length
     *            the number of characters
     * @throws SQLException
     *             if the result set is closed
     */
    @Override
    public void updateNCharacterStream(final String columnName, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("updateNCharacterStream(" + quote(columnName) + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            update(columnName, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {

        debugCode("unwrap");
        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {

        debugCode("isWrapperFor");
        throw Message.getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {

        return getTraceObjectName() + ": " + result;
    }

    private void patchCurrentRow(final Value[] row) throws SQLException {

        boolean changed = false;
        final Value[] current = result.currentRow();
        for (int i = 0; i < row.length; i++) {
            if (!row[i].compareEqual(current[i])) {
                changed = true;
                break;
            }
        }
        if (patchedRows == null) {
            patchedRows = new HashMap();
        }
        final Integer rowId = ObjectUtils.getInteger(result.getRowId());
        if (!changed) {
            patchedRows.remove(rowId);
        }
        else {
            patchedRows.put(rowId, row);
        }
    }

    private Value convertToUnknownValue(final Object x) throws SQLException {

        checkClosed();
        return DataType.convertToValue(conn.getSession(), x, Value.UNKNOWN);
    }

    @Override
    public <T> T getObject(final int arg0, final Class<T> arg1) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public <T> T getObject(final String arg0, final Class<T> arg1) throws SQLException {

        throw Message.getUnsupportedException();
    }

}
