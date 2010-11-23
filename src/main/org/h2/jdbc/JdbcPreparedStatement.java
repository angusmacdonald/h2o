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
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.Calendar;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.expression.ParameterInterface;
import org.h2.message.Message;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.DateTimeUtils;
import org.h2.util.IOUtils;
import org.h2.util.ObjectArray;
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
 * Represents a prepared statement.
 * 
 */
public class JdbcPreparedStatement extends JdbcStatement implements PreparedStatement {

    private final String sql;

    private CommandInterface command;

    private ObjectArray batchParameters;

    JdbcPreparedStatement(final JdbcConnection conn, final String sql, final int resultSetType, final int id, final boolean closeWithResultSet) throws SQLException {

        super(conn, resultSetType, id, closeWithResultSet);
        setTrace(session.getTrace(), TraceObject.PREPARED_STATEMENT, id);
        this.sql = sql;
        command = conn.prepareCommand(sql, fetchSize);
    }

    /**
     * Executes a query (select statement) and returns the result set. If another result set exists for this statement, this will be closed
     * (even if this statement fails).
     * 
     * @return the result set
     * @throws SQLException
     *             if this object is closed or invalid
     */
    @Override
    public ResultSet executeQuery() throws SQLException {

        try {
            final int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSet", TraceObject.RESULT_SET, id, "executeQuery()");
            }
            checkClosed();
            closeOldResultSet();
            ResultInterface result;
            final boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
            synchronized (session) {
                try {
                    setExecutingStatement(command);
                    result = command.executeQuery(maxRows, scrollable);
                }
                finally {
                    setExecutingStatement(null);
                }
            }
            resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable);
            return resultSet;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes a statement (insert, update, delete, create, drop, commit, rollback) and returns the update count. If another result set
     * exists for this statement, this will be closed (even if this statement fails).
     * 
     * If the statement is a create or drop and does not throw an exception, the current transaction (if any) is committed after executing
     * the statement. If auto commit is on, this statement will be committed.
     * 
     * @return the update count (number of row affected by an insert, update or delete, or 0 if no rows or the statement was a create, drop,
     *         commit or rollback)
     * @throws SQLException
     *             if this object is closed or invalid
     */
    @Override
    public int executeUpdate() throws SQLException {

        try {
            debugCodeCall("executeUpdate");
            checkClosed();
            return executeUpdateInternal(false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    private int executeUpdateInternal(final boolean isMultiQueryTransaction) throws SQLException {

        closeOldResultSet();
        synchronized (session) {
            try {
                setExecutingStatement(command);

                updateCount = command.executeUpdate();
                // TODO set to false, so each one runs as a separate transaction
            }
            finally {
                setExecutingStatement(null);
            }
        }
        return updateCount;
    }

    /**
     * Executes an arbitrary statement. If another result set exists for this statement, this will be closed (even if this statement fails).
     * If auto commit is on, and the statement is not a select, this statement will be committed.
     * 
     * @return true if a result set is available, false if not
     * @throws SQLException
     *             if this object is closed or invalid
     */
    @Override
    public boolean execute() throws SQLException {

        try {
            final int id = getNextId(TraceObject.RESULT_SET);
            if (isDebugEnabled()) {
                debugCodeCall("execute");
            }
            checkClosed();
            closeOldResultSet();
            boolean returnsResultSet;
            synchronized (conn.getSession()) {
                try {
                    setExecutingStatement(command);
                    if (command.isQuery()) {
                        returnsResultSet = true;
                        final boolean scrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;
                        final ResultInterface result = command.executeQuery(maxRows, scrollable);
                        resultSet = new JdbcResultSet(conn, this, result, id, closedByResultSet, scrollable);
                    }
                    else {
                        returnsResultSet = false;
                        updateCount = command.executeUpdate();
                    }
                }
                finally {
                    setExecutingStatement(null);
                }
            }
            return returnsResultSet;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears all parameters.
     * 
     * @throws SQLException
     *             if this object is closed or invalid
     */
    @Override
    public void clearParameters() throws SQLException {

        try {
            debugCodeCall("clearParameters");
            checkClosed();
            final ObjectArray parameters = command.getParameters();
            for (int i = 0; i < parameters.size(); i++) {
                final ParameterInterface param = (ParameterInterface) parameters.get(i);
                // can only delete old temp files if they are not in the batch
                param.setValue(null, batchParameters == null);
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {

        try {
            debugCodeCall("executeQuery", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public void addBatch(final String sql) throws SQLException {

        try {
            debugCodeCall("addBatch", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public int executeUpdate(final String sql) throws SQLException {

        try {
            debugCodeCall("executeUpdate", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public boolean execute(final String sql) throws SQLException {

        try {
            debugCodeCall("execute", sql);
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    /**
     * Sets a parameter to null.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param sqlType
     *            the data type (Types.x)
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ");");
            }
            setParameter(parameterIndex, ValueNull.INSTANCE);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setInt(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueInt.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setString(" + parameterIndex + ", " + quote(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBigDecimal(" + parameterIndex + ", " + quoteBigDecimal(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setDate(final int parameterIndex, final java.sql.Date x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setTime(final int parameterIndex, final java.sql.Time x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setTimestamp(final int parameterIndex, final java.sql.Timestamp x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x);");
            }
            if (x == null) {
                // throw Errors.getInvalidValueException("null", "x");
                setParameter(parameterIndex, ValueNull.INSTANCE);
            }
            else {
                setParameter(parameterIndex, DataType.convertToValue(session, x, Value.UNKNOWN));
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to the specified data type before sending to the database.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value, null is allowed
     * @param targetSqlType
     *            the type as defined in java.sql.Types
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ");");
            }
            final int type = DataType.convertSQLTypeToValueType(targetSqlType);
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            }
            else {
                final Value v = DataType.convertToValue(conn.getSession(), x, type);
                setParameter(parameterIndex, v.convertTo(type));
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter. The object is converted, if required, to the specified data type before sending to the database.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value, null is allowed
     * @param targetSqlType
     *            the type as defined in java.sql.Types
     * @param scale
     *            is ignored
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scale) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setObject(" + parameterIndex + ", x, " + targetSqlType + ", " + scale + ");");
            }
            setObject(parameterIndex, x, targetSqlType);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBoolean(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueBoolean.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setByte(" + parameterIndex + ", " + x + ");");
            }
            setParameter(parameterIndex, ValueByte.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setShort(" + parameterIndex + ", (short) " + x + ");");
            }
            setParameter(parameterIndex, ValueShort.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setLong(" + parameterIndex + ", " + x + "L);");
            }
            setParameter(parameterIndex, ValueLong.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setFloat(" + parameterIndex + ", " + x + "f);");
            }
            setParameter(parameterIndex, ValueFloat.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setDouble(" + parameterIndex + ", " + x + "d);");
            }
            setParameter(parameterIndex, ValueDouble.get(x));
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a column as a reference.
     */
    @Override
    public void setRef(final int parameterIndex, final Ref x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setRef(" + parameterIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the date using a specified time zone. The value will be converted to the local time zone.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param calendar
     *            the calendar
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setDate(final int parameterIndex, final java.sql.Date x, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setDate(" + parameterIndex + ", " + quoteDate(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            }
            else {
                setParameter(parameterIndex, DateTimeUtils.convertDateToUniversal(x, calendar));
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the time using a specified time zone. The value will be converted to the local time zone.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param calendar
     *            the calendar
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setTime(final int parameterIndex, final java.sql.Time x, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setTime(" + parameterIndex + ", " + quoteTime(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            }
            else {
                setParameter(parameterIndex, DateTimeUtils.convertTimeToUniversal(x, calendar));
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the timestamp using a specified time zone. The value will be converted to the local time zone.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param calendar
     *            the calendar
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setTimestamp(final int parameterIndex, final java.sql.Timestamp x, final Calendar calendar) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setTimestamp(" + parameterIndex + ", " + quoteTimestamp(x) + ", calendar);");
            }
            if (x == null) {
                setParameter(parameterIndex, ValueNull.INSTANCE);
            }
            else {
                setParameter(parameterIndex, DateTimeUtils.convertTimestampToUniversal(x, calendar));
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] This feature is deprecated and not supported.
     * 
     * @deprecated
     */
    @Deprecated
    @Override
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setUnicodeStream(" + parameterIndex + ", x, " + length + ");");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets a parameter to null.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param sqlType
     *            the data type (Types.x)
     * @param typeName
     *            this parameter is ignored
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNull(" + parameterIndex + ", " + sqlType + ", " + quote(typeName) + ");");
            }
            setNull(parameterIndex, sqlType);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createBlob(x.getBinaryStream(), -1);
            }
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBlob(final int parameterIndex, final InputStream x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, -1);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setClob(final int parameterIndex, final Clob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setClob(final int parameterIndex, final Reader x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createClob(x, -1);
            }
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a Array.
     */
    @Override
    public void setArray(final int parameterIndex, final Array x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setArray(" + parameterIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a byte array.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBytes(" + parameterIndex + ", " + quoteBytes(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBinaryStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an input stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {

        setBinaryStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an input stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException {

        setBinaryStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length) throws SQLException {

        setAsciiStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setAsciiStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(IOUtils.getAsciiReader(x), length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as an ASCII stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {

        setAsciiStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x, final int length) throws SQLException {

        setCharacterStream(parameterIndex, x, (long) length);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x) throws SQLException {

        setCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a character stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setCharacterStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported]
     */
    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setURL(" + parameterIndex + ", x);");
            }
            throw Message.getUnsupportedException();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the result set metadata of the query returned when the statement is executed. If this is not a query, this method returns null.
     * 
     * @return the meta data or null if this is not a query
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {

        try {
            debugCodeCall("getMetaData");
            checkClosed();
            final ResultInterface result = command.getMetaData();
            if (result == null) { return null; }
            final int id = getNextId(TraceObject.RESULT_SET_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ResultSetMetaData", TraceObject.RESULT_SET_META_DATA, id, "getMetaData()");
            }
            final String catalog = conn.getCatalog();
            final JdbcResultSetMetaData meta = new JdbcResultSetMetaData(null, this, result, catalog, session.getTrace(), id);
            return meta;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Clears the batch.
     */
    @Override
    public void clearBatch() throws SQLException {

        try {
            debugCodeCall("clearBatch");
            checkClosed();
            batchParameters = null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Closes this statement. All result sets that where created by this statement become invalid after calling this method.
     */
    @Override
    public void close() throws SQLException {

        try {
            super.close();
            batchParameters = null;
            if (command != null) {
                command.close();
                command = null;
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Executes the batch.
     * 
     * @return the array of update counts
     */
    @Override
    public int[] executeBatch() throws SQLException {

        try {
            debugCodeCall("executeBatch");
            checkClosed();
            if (batchParameters == null) {
                // TODO batch: check what other database do if no parameters are
                // set
                batchParameters = new ObjectArray();
            }

            command.setIsPreparedStatement(true);

            final boolean previousAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            final int[] result = new int[batchParameters.size()];

            boolean error = false;
            SQLException next = null;
            for (int i = 0; i < batchParameters.size(); i++) {
                final ObjectArray parameters = command.getParameters();
                final Value[] set = (Value[]) batchParameters.get(i);
                for (int j = 0; j < set.length; j++) {
                    final Value value = set[j];
                    final ParameterInterface param = (ParameterInterface) parameters.get(j);
                    param.setValue(value, false);
                }
                try {
                    result[i] = executeUpdateInternal(batchParameters.size() > 1);
                    // conn.commit(); //XXX required for the testCoffee test in
                    // BatchTests to pass, but I don't think its the correct
                    // behaviour to commit here.
                }
                catch (final SQLException e) {
                    if (next == null) {
                        next = e;
                    }
                    else {
                        e.setNextException(next);
                        next = e;
                    }
                    logAndConvert(e);
                    // ## Java 1.4 begin ##
                    result[i] = Statement.EXECUTE_FAILED;
                    // ## Java 1.4 end ##
                    error = true;
                }
            }

            final int numOfParameters = batchParameters.size();

            batchParameters = null;
            if (error) {
                // proxyManager.commit(false);
                conn.rollback();
                final JdbcBatchUpdateException e = new JdbcBatchUpdateException(next, result);
                e.setNextException(next);
                throw e;
            }
            else {
                // proxyManager.commit(true);
                conn.commit();
            }

            conn.setAutoCommit(previousAutoCommit);

            return result;
        }
        catch (final Exception e) {
            // if (proxyManager != null) proxyManager.commit(false);
            throw logAndConvert(e);
        }
    }

    /**
     * Adds the current settings to the batch.
     */
    @Override
    public void addBatch() throws SQLException {

        try {
            debugCodeCall("addBatch");
            checkClosed();
            final ObjectArray parameters = command.getParameters();
            final Value[] set = new Value[parameters.size()];
            for (int i = 0; i < parameters.size(); i++) {
                final ParameterInterface param = (ParameterInterface) parameters.get(i);
                final Value value = param.getParamValue();
                set[i] = value;
            }
            if (batchParameters == null) {
                batchParameters = new ObjectArray();
            }
            batchParameters.add(set);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("executeUpdate(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Calling this method is not legal on a PreparedStatement.
     * 
     * @throws SQLException
     *             Unsupported Feature
     */
    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("execute(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Get the parameter meta data of this prepared statement.
     * 
     * @return the meta data
     */
    // ## Java 1.4 begin ##
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {

        try {
            final int id = getNextId(TraceObject.PARAMETER_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("ParameterMetaData", TraceObject.PARAMETER_META_DATA, id, "getParameterMetaData()");
            }
            checkClosed();
            final JdbcParameterMetaData meta = new JdbcParameterMetaData(session.getTrace(), this, command, id);
            return meta;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // ## Java 1.4 end ##

    // =============================================================

    private void setParameter(int parameterIndex, final Value value) throws SQLException {

        checkClosed();
        parameterIndex--;
        final ObjectArray parameters = command.getParameters();
        if (parameterIndex < 0 || parameterIndex >= parameters.size()) { throw Message.getInvalidValueException("" + (parameterIndex + 1), "parameterIndex"); }
        final ParameterInterface param = (ParameterInterface) parameters.get(parameterIndex);
        // can only delete old temp files if they are not in the batch
        param.setValue(value, batchParameters == null);
    }

    /**
     * [Not supported] Sets the value of a parameter as a row id.
     */

    @Override
    public void setRowId(final int parameterIndex, final RowId x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Sets the value of a parameter.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNString(final int parameterIndex, final String x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNString(" + parameterIndex + ", " + quote(x) + ");");
            }
            final Value v = x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @param length
     *            the number of bytes
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNCharacterStream(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a character stream.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNCharacterStream(final int parameterIndex, final Reader x) throws SQLException {

        setNCharacterStream(parameterIndex, x, -1);
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */

    @Override
    public void setNClob(final int parameterIndex, final NClob x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            Value v;
            if (x == null) {
                v = ValueNull.INSTANCE;
            }
            else {
                v = conn.createClob(x.getCharacterStream(), -1);
            }
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNClob(final int parameterIndex, final Reader x) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x);");
            }
            checkClosed();
            final Value v = conn.createClob(x, -1);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setClob(final int parameterIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setClob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Blob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setBlob(final int parameterIndex, final InputStream x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setBlob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createBlob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Sets the value of a parameter as a Clob.
     * 
     * @param parameterIndex
     *            the parameter index (1, 2, ...)
     * @param x
     *            the value
     * @throws SQLException
     *             if this object is closed
     */
    @Override
    public void setNClob(final int parameterIndex, final Reader x, final long length) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setNClob(" + parameterIndex + ", x, " + length + "L);");
            }
            checkClosed();
            final Value v = conn.createClob(x, length);
            setParameter(parameterIndex, v);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Sets the value of a parameter as a SQLXML object.
     */

    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {

        return getTraceObjectName() + ": " + command;
    }

    @Override
    boolean checkClosed() throws SQLException {

        if (super.checkClosed()) {
            // if the session was re-connected, re-prepare the statement
            command = conn.prepareCommand(sql, fetchSize);
            return true;
        }
        return false;
    }

}
