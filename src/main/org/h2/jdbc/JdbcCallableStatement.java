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
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.h2.message.Message;
import org.h2.message.TraceObject;

/**
 * Represents a callable statement.
 */
public class JdbcCallableStatement extends JdbcPreparedStatement implements CallableStatement {

    JdbcCallableStatement(final JdbcConnection conn, final String sql, final int resultSetType, final int id) throws SQLException {

        super(conn, sql, resultSetType, id, false);
        setTrace(session.getTrace(), TraceObject.CALLABLE_STATEMENT, id);
    }

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final int parameterIndex, final int sqlType) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final int parameterIndex, final int sqlType, final int scale) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public boolean wasNull() throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public String getString(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public boolean getBoolean(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public byte getByte(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public short getShort(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public int getInt(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public long getLong(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public float getFloat(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public double getDouble(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     * 
     * @deprecated
     */
    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final int parameterIndex, final int scale) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public byte[] getBytes(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Date getDate(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Time getTime(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Timestamp getTimestamp(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Object getObject(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public BigDecimal getBigDecimal(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Ref getRef(final int i) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Blob getBlob(final int i) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Clob getClob(final int i) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Array getArray(final int i) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Date getDate(final int parameterIndex, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Time getTime(final int parameterIndex, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Timestamp getTimestamp(final int parameterIndex, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final int paramIndex, final int sqlType, final String typeName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     * 
     */
    @Override
    public URL getURL(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     * 
     */
    @Override
    public Timestamp getTimestamp(final String parameterName, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Time getTime(final String parameterName, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Date getDate(final String parameterName, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Array getArray(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Clob getClob(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Blob getBlob(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Ref getRef(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public BigDecimal getBigDecimal(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Object getObject(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Timestamp getTimestamp(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Time getTime(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public Date getDate(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public byte[] getBytes(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public double getDouble(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public float getFloat(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public long getLong(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public int getInt(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public short getShort(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public byte getByte(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public boolean getBoolean(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public String getString(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    // --- setters --------------------------------------------------

    /**
     * [Not supported]
     */
    @Override
    public void setNull(final String parameterName, final int sqlType, final String typeName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setTimestamp(final String parameterName, final Timestamp x, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setTime(final String parameterName, final Time x, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setDate(final String parameterName, final Date x, final Calendar cal) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setCharacterStream(final String parameterName, final Reader reader, final int length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setObject(final String parameterName, final Object x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setObject(final String parameterName, final Object x, final int targetSqlType) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setObject(final String parameterName, final Object x, final int targetSqlType, final int scale) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setBinaryStream(final String parameterName, final InputStream x, final int length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setAsciiStream(final String parameterName, final InputStream x, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setTimestamp(final String parameterName, final Timestamp x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setTime(final String parameterName, final Time x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setDate(final String parameterName, final Date x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setBytes(final String parameterName, final byte[] x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setString(final String parameterName, final String x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setBigDecimal(final String parameterName, final BigDecimal x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setDouble(final String parameterName, final double x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setFloat(final String parameterName, final float x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setLong(final String parameterName, final long x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setInt(final String parameterName, final int x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setShort(final String parameterName, final short x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setByte(final String parameterName, final byte x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setBoolean(final String parameterName, final boolean x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setNull(final String parameterName, final int sqlType) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setURL(final String parameterName, final URL val) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public URL getURL(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    // --- other methods --------------------------------------------

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final String parameterName, final int sqlType, final String typeName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final String parameterName, final int sqlType, final int scale) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void registerOutParameter(final String parameterName, final int sqlType) throws SQLException {

        throw Message.getUnsupportedException();
    }

    // =============================================================

    /**
     * [Not supported]
     */

    @Override
    public RowId getRowId(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public RowId getRowId(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setRowId(final String parameterName, final RowId x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNString(final String parameterName, final String value) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNCharacterStream(final String parameterName, final Reader value, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNClob(final String parameterName, final NClob value) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setClob(final String parameterName, final Reader reader, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setBlob(final String parameterName, final InputStream inputStream, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNClob(final String parameterName, final Reader reader, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public NClob getNClob(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public NClob getNClob(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setSQLXML(final String parameterName, final SQLXML xmlObject) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public SQLXML getSQLXML(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public SQLXML getSQLXML(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public String getNString(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public String getNString(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public Reader getNCharacterStream(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public Reader getNCharacterStream(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public Reader getCharacterStream(final int parameterIndex) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public Reader getCharacterStream(final String parameterName) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setBlob(final String parameterName, final Blob x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setClob(final String parameterName, final Clob x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setAsciiStream(final String parameterName, final InputStream x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */
    @Override
    public void setAsciiStream(final String parameterName, final InputStream x, final int length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setBinaryStream(final String parameterName, final InputStream x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setBinaryStream(final String parameterName, final InputStream x, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setBlob(final String parameterName, final InputStream x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setCharacterStream(final String parameterName, final Reader x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setCharacterStream(final String parameterName, final Reader x, final long length) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setClob(final String parameterName, final Reader x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNCharacterStream(final String parameterName, final Reader x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported]
     */

    @Override
    public void setNClob(final String parameterName, final Reader x) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

        // TODO Auto-generated method stub

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public Object getObject(final int arg0, final Map<String, Class<?>> arg1) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public Object getObject(final String arg0, final Map<String, Class<?>> arg1) throws SQLException {

        throw Message.getUnsupportedException();
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
