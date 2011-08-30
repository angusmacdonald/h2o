/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.h2.command.CommandInterface;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Constants;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.JdbcConnectionListener;
import org.h2.util.ObjectUtils;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * <p>
 * Represents a connection (session) to a database.
 * </p>
 * <p>
 * Thread safety: The connection is thread-safe, because access is synchronized. However, for compatibility with other databases, a
 * connection should only be used in one thread at any time.
 * </p>
 */
public class JdbcConnection extends TraceObject implements Connection {

    private String url;

    private String user;

    // ResultSet.HOLD_CURSORS_OVER_COMMIT
    private int holdability = 1;

    private SessionInterface session;

    private CommandInterface commit, rollback;

    private CommandInterface setAutoCommitTrue, setAutoCommitFalse, getAutoCommit;

    private CommandInterface getReadOnly, getGeneratedKeys;

    private CommandInterface setLockMode, getLockMode;

    private CommandInterface setQueryTimeout, getQueryTimeout;

    private Exception openStackTrace;

    // ## Java 1.4 begin ##
    private int savepointId;

    // ## Java 1.4 end ##
    private Trace trace;

    private JdbcConnectionListener listener;

    private boolean isInternal;

    private String catalog;

    private Statement executingStatement;

    /**
     * INTERNAL
     */
    public JdbcConnection(final String url, final Properties info) throws SQLException {

        this(new ConnectionInfo(url, info), true);
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(final ConnectionInfo ci, final boolean useBaseDir) throws SQLException {

        try {
            if (useBaseDir) {
                final String baseDir = SysProperties.getBaseDir();
                if (baseDir != null) {
                    ci.setBaseDir(baseDir);
                }
            }
            checkJavaVersion();
            // this will return an embedded or server connection
            session = new SessionRemote().createSession(ci);
            trace = session.getTrace();
            final int id = getNextId(TraceObject.CONNECTION);
            setTrace(trace, TraceObject.CONNECTION, id);
            user = ci.getUserName();
            if (isInfoEnabled()) {
                trace.infoCode("Connection " + getTraceObjectName() + " = DriverManager.getConnection(" + quote(ci.getOriginalURL()) + ", " + quote(user) + ", \"\")");
            }
            url = ci.getURL();
            openStackTrace = new Exception("Stack Trace");
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public JdbcConnection(final SessionInterface session, final String user, final String url) {

        isInternal = true;
        this.session = session;
        trace = session.getTrace();
        final int id = getNextId(TraceObject.CONNECTION);
        setTrace(trace, TraceObject.CONNECTION, id);
        this.user = user;
        this.url = url;
    }

    /**
     * Creates a new statement.
     * 
     * @return the new statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public Statement createStatement() throws SQLException {

        try {
            final int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement()");
            }
            checkClosed();
            return new JdbcStatement(this, ResultSet.TYPE_FORWARD_ONLY, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type and concurrency.
     * 
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type or concurrency are not supported
     */
    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency) throws SQLException {

        try {
            final int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement(" + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            return new JdbcStatement(this, resultSetType, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a statement with the specified result set type, concurrency, and holdability.
     * 
     * @return the statement
     * @throws SQLException
     *             if the connection is closed or the result set type, concurrency, or holdability are not supported
     */
    @Override
    public Statement createStatement(final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {

        try {
            final int id = getNextId(TraceObject.STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("Statement", TraceObject.STATEMENT, id, "createStatement(" + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkClosed();
            checkHoldability(resultSetHoldability);
            return new JdbcStatement(this, resultSetType, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement.
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {

        try {
            final int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Prepare a statement that will automatically close when the result set is closed. This method is used to retrieve database meta data.
     * 
     * @param sql
     *            the SQL statement.
     * @return the prepared statement
     */
    PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {

        try {
            final int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY, id, true);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the database meta data for this database.
     * 
     * @return the database meta data
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {

        try {
            final int id = getNextId(TraceObject.DATABASE_META_DATA);
            if (isDebugEnabled()) {
                debugCodeAssign("DatabaseMetaData", TraceObject.DATABASE_META_DATA, id, "getMetaData()");
            }
            checkClosed();
            return new JdbcDatabaseMetaData(this, trace, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setJdbcConnectionListener(final JdbcConnectionListener listener) {

        this.listener = listener;
    }

    /**
     * INTERNAL
     */
    public SessionInterface getSession() {

        return session;
    }

    /**
     * Closes this connection. All open statements, prepared statements and result sets that where created by this connection become invalid
     * after calling this method. If there is an uncommitted transaction, it will be rolled back.
     */
    @Override
    public void close() throws SQLException {

        synchronized (this) {
            if (listener == null) {
                closeConnection();
            }
            else {
                listener.closed(this);
            }
        }
    }

    /**
     * INTERNAL
     */
    public void closeConnection() throws SQLException {

        try {
            debugCodeCall("close");
            if (executingStatement != null) {
                executingStatement.cancel();
            }
            if (session == null) { return; }
            session.cancel();
            try {
                synchronized (session) {
                    if (!session.isClosed()) {
                        try {
                            rollbackInternal();
                            commit = closeAndSetNull(commit);
                            rollback = closeAndSetNull(rollback);
                            setAutoCommitTrue = closeAndSetNull(setAutoCommitTrue);
                            setAutoCommitFalse = closeAndSetNull(setAutoCommitFalse);
                            getAutoCommit = closeAndSetNull(getAutoCommit);
                            getReadOnly = closeAndSetNull(getReadOnly);
                            getGeneratedKeys = closeAndSetNull(getGeneratedKeys);
                            getLockMode = closeAndSetNull(getLockMode);
                            setLockMode = closeAndSetNull(setLockMode);
                            getQueryTimeout = closeAndSetNull(getQueryTimeout);
                            setQueryTimeout = closeAndSetNull(setQueryTimeout);
                        }
                        finally {
                            session.close();
                        }
                    }
                }
            }
            finally {
                session = null;
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    private CommandInterface closeAndSetNull(final CommandInterface command) {

        if (command != null) {
            command.close();
        }
        return null;
    }

    /**
     * Switches auto commit on or off. Calling this function does not commit the current transaction.
     * 
     * @param autoCommit
     *            true for auto commit on, false for off
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public synchronized void setAutoCommit(final boolean autoCommit) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setAutoCommit(" + autoCommit + ");");
            }
            checkClosed();
            if (autoCommit) {
                setAutoCommitTrue = prepareCommand("SET AUTOCOMMIT TRUE", setAutoCommitTrue);
                setAutoCommitTrue.executeUpdate();
            }
            else {
                setAutoCommitFalse = prepareCommand("SET AUTOCOMMIT FALSE", setAutoCommitFalse);
                setAutoCommitFalse.executeUpdate();
            }
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current setting for auto commit.
     * 
     * @return true for on, false for off
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public synchronized boolean getAutoCommit() throws SQLException {

        try {
            checkClosed();
            debugCodeCall("getAutoCommit");
            return getInternalAutoCommit();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    private boolean getInternalAutoCommit() throws SQLException {

        getAutoCommit = prepareCommand("CALL AUTOCOMMIT()", getAutoCommit);
        final ResultInterface result = getAutoCommit.executeQuery(0, false);
        result.next();
        final boolean autoCommit = result.currentRow()[0].getBoolean().booleanValue();
        result.close();
        return autoCommit;
    }

    /**
     * Commits the current transaction. This call has only an effect if auto commit is switched off.
     * 
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public synchronized void commit() throws SQLException {

        try {
            debugCodeCall("commit");
            checkClosed();
            commit = prepareCommand("COMMIT", commit);
            commit.executeUpdate();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Rolls back the current transaction. This call has only an effect if auto commit is switched off.
     * 
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public synchronized void rollback() throws SQLException {

        try {
            debugCodeCall("rollback");
            checkClosed();
            rollbackInternal();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if this connection has been closed.
     * 
     * @return true if close was called
     */
    @Override
    public boolean isClosed() throws SQLException {

        try {
            debugCodeCall("isClosed");
            return session == null || session.isClosed();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Translates a SQL statement into the database grammar.
     * 
     * @return the translated statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public String nativeSQL(final String sql) throws SQLException {

        try {
            debugCodeCall("nativeSQL", sql);
            checkClosed();
            return translateSQL(sql);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * According to the JDBC specs, this setting is only a hint to the database to enable optimizations - it does not cause writes to be
     * prohibited.
     * 
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("setReadOnly(" + readOnly + ");");
            }
            checkClosed();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns true if the database is read-only.
     * 
     * @return if the database is read-only
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public boolean isReadOnly() throws SQLException {

        try {
            debugCodeCall("isReadOnly");
            checkClosed();
            getReadOnly = prepareCommand("CALL READONLY()", getReadOnly);
            final ResultInterface result = getReadOnly.executeQuery(0, false);
            result.next();
            final boolean readOnly = result.currentRow()[0].getBoolean().booleanValue();
            return readOnly;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Set the default catalog name. This call is ignored.
     * 
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public void setCatalog(final String catalog) throws SQLException {

        try {
            debugCodeCall("setCatalog", catalog);
            checkClosed();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the current catalog name.
     * 
     * @return the catalog name
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public String getCatalog() throws SQLException {

        try {
            debugCodeCall("getCatalog");
            checkClosed();
            if (catalog == null) {
                final CommandInterface cat = prepareCommand("CALL DATABASE()", Integer.MAX_VALUE);
                final ResultInterface result = cat.executeQuery(0, false);
                result.next();
                catalog = result.currentRow()[0].getString();
                cat.close();
            }
            return catalog;
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

    /**
     * Creates a prepared statement with the specified result set type and concurrency.
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type or concurrency are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {

        try {
            final int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, resultSetType, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current transaction isolation level. Calling this method will commit an open transaction, even if the new level is the
     * same as the old one, except if the level is not supported. Internally, this method calls SET LOCK_MODE. The following isolation
     * levels are supported:
     * <ul>
     * <li>Connection.TRANSACTION_READ_UNCOMMITTED = SET LOCK_MODE 0: No locking (should only be used for testing).</li>
     * <li>Connection.TRANSACTION_SERIALIZABLE = SET LOCK_MODE 1: Table level locking.</li>
     * <li>Connection.TRANSACTION_READ_COMMITTED = SET LOCK_MODE 3: Table level locking, but read locks are released immediately (default).</li>
     * </ul>
     * This setting is not persistent. Please note that using TRANSACTION_READ_UNCOMMITTED while at the same time using multiple connections
     * may result in inconsistent transactions.
     * 
     * @param level
     *            the new transaction isolation level: Connection.TRANSACTION_READ_UNCOMMITTED, Connection.TRANSACTION_READ_COMMITTED, or
     *            Connection.TRANSACTION_SERIALIZABLE
     * @throws SQLException
     *             if the connection is closed or the isolation level is not supported
     */
    @Override
    public void setTransactionIsolation(final int level) throws SQLException {

        try {
            debugCodeCall("setTransactionIsolation", level);
            checkClosed();
            int lockMode;
            switch (level) {
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    lockMode = Constants.LOCK_MODE_OFF;
                    break;
                case Connection.TRANSACTION_READ_COMMITTED:
                    lockMode = Constants.LOCK_MODE_READ_COMMITTED;
                    break;
                case Connection.TRANSACTION_REPEATABLE_READ:
                case Connection.TRANSACTION_SERIALIZABLE:
                    lockMode = Constants.LOCK_MODE_TABLE;
                    break;
                default:
                    throw Message.getInvalidValueException("" + level, "level");
            }
            commit();
            setLockMode = prepareCommand("SET LOCK_MODE ?", setLockMode);
            ((ParameterInterface) setLockMode.getParameters().get(0)).setValue(ValueInt.get(lockMode), false);
            setLockMode.executeUpdate();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public void setQueryTimeout(final int seconds) throws SQLException {

        try {
            setQueryTimeout = prepareCommand("SET QUERY_TIMEOUT ?", setQueryTimeout);
            ((ParameterInterface) setQueryTimeout.getParameters().get(0)).setValue(ValueInt.get(seconds * 1000), false);
            setQueryTimeout.executeUpdate();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * INTERNAL
     */
    public int getQueryTimeout() throws SQLException {

        try {
            getQueryTimeout = prepareCommand("SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME=?", getQueryTimeout);
            ((ParameterInterface) getQueryTimeout.getParameters().get(0)).setValue(ValueString.get("QUERY_TIMEOUT"), false);
            final ResultInterface result = getQueryTimeout.executeQuery(0, false);
            result.next();
            final int queryTimeout = result.currentRow()[0].getInt();
            result.close();
            if (queryTimeout == 0) { return 0; }
            // round to the next second, otherwise 999 millis would return 0
            // seconds
            return (queryTimeout + 999) / 1000;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }

    }

    /**
     * Returns the current transaction isolation level.
     * 
     * @return the isolation level.
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public int getTransactionIsolation() throws SQLException {

        try {
            debugCodeCall("getTransactionIsolation");
            checkClosed();
            getLockMode = prepareCommand("CALL LOCK_MODE()", getLockMode);
            final ResultInterface result = getLockMode.executeQuery(0, false);
            result.next();
            final int lockMode = result.currentRow()[0].getInt();
            result.close();
            int transactionIsolationLevel;
            switch (lockMode) {
                case Constants.LOCK_MODE_OFF:
                    transactionIsolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
                    break;
                case Constants.LOCK_MODE_READ_COMMITTED:
                    transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
                    break;
                case Constants.LOCK_MODE_TABLE:
                case Constants.LOCK_MODE_TABLE_GC:
                    transactionIsolationLevel = Connection.TRANSACTION_SERIALIZABLE;
                    break;
                default:
                    throw Message.throwInternalError("lockMode:" + lockMode);
            }
            return transactionIsolationLevel;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Changes the current result set holdability.
     * 
     * @param holdability
     *            ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT;
     * @throws SQLException
     *             if the connection is closed or the holdability is not supported
     */
    @Override
    public void setHoldability(final int holdability) throws SQLException {

        try {
            debugCodeCall("setHoldability", holdability);
            checkClosed();
            checkHoldability(holdability);
            this.holdability = holdability;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
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
            return holdability;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the type map.
     * 
     * @return null
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public Map getTypeMap() throws SQLException {

        try {
            debugCodeCall("getTypeMap");
            checkClosed();
            return null;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Partially supported] Sets the type map. This is only supported if the map is empty or null.
     */
    @Override
    public void setTypeMap(final Map map) throws SQLException {

        try {
            debugCode("setTypeMap(" + quoteMap(map) + ");");
            checkMap(map);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new callable statement.
     * 
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the statement is not valid
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type and concurrency.
     * 
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type or concurrency are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, final int resultSetType, final int resultSetConcurrency) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ")");
            }
            checkClosed();
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, resultSetType, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a callable statement with the specified result set type, concurrency, and holdability.
     * 
     * @return the callable statement
     * @throws SQLException
     *             if the connection is closed or the result set type, concurrency, or holdability are not supported
     */
    @Override
    public CallableStatement prepareCall(String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {

        try {
            final int id = getNextId(TraceObject.CALLABLE_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("CallableStatement", TraceObject.CALLABLE_STATEMENT, id, "prepareCall(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkClosed();
            checkHoldability(resultSetHoldability);
            sql = translateSQL(sql);
            return new JdbcCallableStatement(this, sql, resultSetType, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new unnamed savepoint.
     * 
     * @return the new savepoint
     */
    // ## Java 1.4 begin ##
    @Override
    public Savepoint setSavepoint() throws SQLException {

        try {
            final int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint()");
            }
            checkClosed();
            final CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(null, savepointId), Integer.MAX_VALUE);
            set.executeUpdate();
            final JdbcSavepoint savepoint = new JdbcSavepoint(this, savepointId, null, trace, id);
            savepointId++;
            return savepoint;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // ## Java 1.4 end ##

    /**
     * Creates a new named savepoint.
     * 
     * @param name
     *            the savepoint name
     * @return the new savepoint
     */
    // ## Java 1.4 begin ##
    @Override
    public Savepoint setSavepoint(final String name) throws SQLException {

        try {
            final int id = getNextId(TraceObject.SAVEPOINT);
            if (isDebugEnabled()) {
                debugCodeAssign("Savepoint", TraceObject.SAVEPOINT, id, "setSavepoint(" + quote(name) + ")");
            }
            checkClosed();
            final CommandInterface set = prepareCommand("SAVEPOINT " + JdbcSavepoint.getName(name, 0), Integer.MAX_VALUE);
            set.executeUpdate();
            final JdbcSavepoint savepoint = new JdbcSavepoint(this, 0, name, trace, id);
            return savepoint;
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // ## Java 1.4 end ##

    /**
     * Rolls back to a savepoint.
     * 
     * @param savepoint
     *            the savepoint
     */
    // ## Java 1.4 begin ##
    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {

        try {
            final JdbcSavepoint sp = convertSavepoint(savepoint);
            debugCode("rollback(" + sp.getTraceObjectName() + ");");
            checkClosed();
            sp.rollback();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // ## Java 1.4 end ##

    /**
     * Releases a savepoint.
     * 
     * @param savepoint
     *            the savepoint to release
     */
    // ## Java 1.4 begin ##
    @Override
    public void releaseSavepoint(final Savepoint savepoint) throws SQLException {

        try {
            debugCode("releaseSavepoint(savepoint);");
            checkClosed();
            convertSavepoint(savepoint).release();
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    private JdbcSavepoint convertSavepoint(final Savepoint savepoint) throws SQLException {

        if (!(savepoint instanceof JdbcSavepoint)) { throw Message.getSQLException(ErrorCode.SAVEPOINT_IS_INVALID_1, "" + savepoint); }
        return (JdbcSavepoint) savepoint;
    }

    // ## Java 1.4 end ##

    /**
     * Creates a prepared statement with the specified result set type, concurrency, and holdability.
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed or the result set type, concurrency, or holdability are not supported
     */
    @Override
    public PreparedStatement prepareStatement(String sql, final int resultSetType, final int resultSetConcurrency, final int resultSetHoldability) throws SQLException {

        try {
            final int id = getNextId(TraceObject.PREPARED_STATEMENT);
            if (isDebugEnabled()) {
                debugCodeAssign("PreparedStatement", TraceObject.PREPARED_STATEMENT, id, "prepareStatement(" + quote(sql) + ", " + resultSetType + ", " + resultSetConcurrency + ", " + resultSetHoldability + ")");
            }
            checkClosed();
            checkHoldability(resultSetHoldability);
            sql = translateSQL(sql);
            return new JdbcPreparedStatement(this, sql, resultSetType, id, false);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement. This method just calls prepareStatement(String sql).
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int autoGeneratedKeys) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + autoGeneratedKeys + ");");
            }
            return prepareStatement(sql);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement. This method just calls prepareStatement(String sql).
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final int[] columnIndexes) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteIntArray(columnIndexes) + ");");
            }
            return prepareStatement(sql);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Creates a new prepared statement. This method just calls prepareStatement(String sql).
     * 
     * @return the prepared statement
     * @throws SQLException
     *             if the connection is closed
     */
    @Override
    public PreparedStatement prepareStatement(final String sql, final String[] columnNames) throws SQLException {

        try {
            if (isDebugEnabled()) {
                debugCode("prepareStatement(" + quote(sql) + ", " + quoteArray(columnNames) + ");");
            }
            return prepareStatement(sql);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    // =============================================================

    private void checkJavaVersion() throws SQLException {

        try {
            // ## Java 1.4 begin ##
            // check for existence of this class (avoiding Class . forName)
            final Class clazz = java.sql.Savepoint.class;
            clazz.getClass();
            // ## Java 1.4 end ##
        }
        catch (final NoClassDefFoundError e) {
            throw Message.getSQLException(ErrorCode.UNSUPPORTED_JAVA_VERSION);
        }
    }

    /**
     * Prepare an command. This will parse the SQL statement.
     * 
     * @param sql
     *            the SQL statement
     * @param fetchSize
     *            the fetch size (used in remote connections)
     * @return the command
     */
    CommandInterface prepareCommand(final String sql, final int fetchSize) throws SQLException {

        return session.prepareCommand(sql, fetchSize);
    }

    private CommandInterface prepareCommand(final String sql, final CommandInterface old) throws SQLException {

        return old == null ? session.prepareCommand(sql, Integer.MAX_VALUE) : old;
    }

    private int translateGetEnd(final String sql, int i, char c) throws SQLException {

        final int len = sql.length();
        switch (c) {
            case '$': {
                if (i < len - 1 && sql.charAt(i + 1) == '$' && (i == 0 || sql.charAt(i - 1) <= ' ')) {
                    final int j = sql.indexOf("$$", i + 2);
                    if (j < 0) { throw Message.getSyntaxError(sql, i); }
                    return j + 1;
                }
                return i;
            }
            case '\'': {
                final int j = sql.indexOf('\'', i + 1);
                if (j < 0) { throw Message.getSyntaxError(sql, i); }
                return j;
            }
            case '"': {
                final int j = sql.indexOf('"', i + 1);
                if (j < 0) { throw Message.getSyntaxError(sql, i); }
                return j;
            }
            case '/': {
                checkRunOver(i + 1, len, sql);
                if (sql.charAt(i + 1) == '*') {
                    // block comment
                    final int j = sql.indexOf("*/", i + 2);
                    if (j < 0) { throw Message.getSyntaxError(sql, i); }
                    i = j + 1;
                }
                else if (sql.charAt(i + 1) == '/') {
                    // single line comment
                    i += 2;
                    while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                        i++;
                    }
                }
                return i;
            }
            case '-': {
                checkRunOver(i + 1, len, sql);
                if (sql.charAt(i + 1) == '-') {
                    // single line comment
                    i += 2;
                    while (i < len && (c = sql.charAt(i)) != '\r' && c != '\n') {
                        i++;
                    }
                }
                return i;
            }
            default:
                throw Message.throwInternalError("c=" + c);
        }
    }

    /**
     * Convert JDBC escape sequences in the SQL statement.
     * 
     * @param sql
     *            the SQL statement with or without JDBC escape sequences
     * @return the SQL statement without JDBC escape sequences
     */
    String translateSQL(String sql) throws SQLException {

        if (sql == null || sql.indexOf('{') < 0) { return sql; }
        final int len = sql.length();
        char[] chars = null;
        int level = 0;
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '\'':
                case '"':
                case '/':
                case '-':
                    i = translateGetEnd(sql, i, c);
                    break;
                case '{':
                    level++;
                    if (chars == null) {
                        chars = sql.toCharArray();
                    }
                    chars[i] = ' ';
                    while (Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                    final int start = i;
                    if (chars[i] >= '0' && chars[i] <= '9') {
                        chars[i - 1] = '{';
                        while (true) {
                            checkRunOver(i, len, sql);
                            c = chars[i];
                            if (c == '}') {
                                break;
                            }
                            switch (c) {
                                case '\'':
                                case '"':
                                case '/':
                                case '-':
                                    i = translateGetEnd(sql, i, c);
                                    break;
                                default:
                            }
                            i++;
                        }
                        level--;
                        break;
                    }
                    else if (chars[i] == '?') {
                        chars[i++] = ' ';
                        checkRunOver(i, len, sql);
                        while (Character.isSpaceChar(chars[i])) {
                            i++;
                            checkRunOver(i, len, sql);
                        }
                        if (sql.charAt(i) != '=') { throw Message.getSyntaxError(sql, i, "="); }
                        chars[i++] = ' ';
                        checkRunOver(i, len, sql);
                        while (Character.isSpaceChar(chars[i])) {
                            i++;
                            checkRunOver(i, len, sql);
                        }
                    }
                    while (!Character.isSpaceChar(chars[i])) {
                        i++;
                        checkRunOver(i, len, sql);
                    }
                    int remove = 0;
                    if (found(sql, start, "fn")) {
                        remove = 2;
                    }
                    else if (found(sql, start, "escape")) {
                        break;
                    }
                    else if (found(sql, start, "call")) {
                        break;
                    }
                    else if (found(sql, start, "oj")) {
                        remove = 2;
                    }
                    else if (found(sql, start, "ts")) {
                        remove = 2;
                    }
                    else if (found(sql, start, "t")) {
                        remove = 1;
                    }
                    else if (found(sql, start, "d")) {
                        remove = 1;
                    }
                    else if (found(sql, start, "params")) {
                        remove = "params".length();
                    }
                    for (i = start; remove > 0; i++, remove--) {
                        chars[i] = ' ';
                    }
                    break;
                case '}':
                    if (--level < 0) { throw Message.getSyntaxError(sql, i); }
                    chars[i] = ' ';
                    break;
                case '$':
                    if (SysProperties.DOLLAR_QUOTING) {
                        i = translateGetEnd(sql, i, c);
                    }
                    break;
                default:
            }
        }
        if (level != 0) { throw Message.getSyntaxError(sql, sql.length() - 1); }
        if (chars != null) {
            sql = new String(chars);
        }
        return sql;
    }

    private void checkRunOver(final int i, final int len, final String sql) throws SQLException {

        if (i >= len) { throw Message.getSyntaxError(sql, i); }
    }

    private boolean found(final String sql, final int start, final String other) {

        return sql.regionMatches(true, start, other, 0, other.length());
    }

    private void checkHoldability(final int resultSetHoldability) throws SQLException {

        // TODO compatibility / correctness: DBPool uses
        // ResultSet.HOLD_CURSORS_OVER_COMMIT
        // ## Java 1.4 begin ##
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) { throw Message.getInvalidValueException("" + resultSetHoldability, "resultSetHoldability"); }
        // ## Java 1.4 end ##
    }

    /**
     * Check if this connection is closed.
     * 
     * @throws SQLException
     *             if the connection or session is closed
     */
    boolean checkClosed() throws SQLException {

        if (session == null) { throw Message.getSQLException(ErrorCode.OBJECT_CLOSED); }
        if (session.isClosed()) { throw new SQLException("Database is already closed: " + url); }
        if (session.isReconnectNeeded()) {
            trace.debug("reconnect");
            session = session.reconnect();
            setTrace(session.getTrace());
            return true;
        }
        return false;
    }

    String getURL() throws SQLException {

        checkClosed();
        return url;
    }

    String getUser() throws SQLException {

        checkClosed();
        return user;
    }

    @Override
    protected void finalize() {

        if (!SysProperties.runFinalize) { return; }
        if (isInternal) { return; }
        if (session != null) {
            trace.error("Connection not closed", openStackTrace);
            try {
                close();
            }
            catch (final SQLException e) {
                trace.debug("finalize", e);
            }
        }
    }

    private void rollbackInternal() throws SQLException {

        rollback = prepareCommand("ROLLBACK", rollback);
        rollback.executeUpdate();
    }

    /**
     * INTERNAL
     */
    public int getPowerOffCount() {

        return session == null || session.isClosed() ? 0 : session.getPowerOffCount();
    }

    /**
     * INTERNAL
     */
    public void setPowerOffCount(final int count) throws SQLException {

        if (session != null) {
            session.setPowerOffCount(count);
        }
    }

    /**
     * INTERNAL
     */
    public void setExecutingStatement(final Statement stat) {

        executingStatement = stat;
    }

    ResultInterface getGeneratedKeys() throws SQLException {

        getGeneratedKeys = prepareCommand("CALL IDENTITY()", getGeneratedKeys);
        return getGeneratedKeys.executeQuery(0, false);
    }

    /**
     * Create a new empty Clob object.
     * 
     * @return the object
     */
    @Override
    public Clob createClob() throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("Clob", TraceObject.CLOB, id, "createClob()");
            checkClosed();
            final ValueLob v = ValueLob.createSmallLob(Value.CLOB, new byte[0]);
            return new JdbcClob(this, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty Blob object.
     * 
     * @return the object
     */
    @Override
    public Blob createBlob() throws SQLException {

        try {
            final int id = getNextId(TraceObject.BLOB);
            debugCodeAssign("Blob", TraceObject.BLOB, id, "createClob()");
            checkClosed();
            final ValueLob v = ValueLob.createSmallLob(Value.BLOB, new byte[0]);
            return new JdbcBlob(this, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Create a new empty NClob object.
     * 
     * @return the object
     */

    @Override
    public NClob createNClob() throws SQLException {

        try {
            final int id = getNextId(TraceObject.CLOB);
            debugCodeAssign("NClob", TraceObject.CLOB, id, "createNClob()");
            checkClosed();
            final ValueLob v = ValueLob.createSmallLob(Value.CLOB, new byte[0]);
            return new JdbcClob(this, v, id);
        }
        catch (final Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * [Not supported] Create a new empty SQLXML object.
     */

    @Override
    public SQLXML createSQLXML() throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Create a new empty Array object.
     */

    @Override
    public Array createArrayOf(final String typeName, final Object[] elements) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Create a new empty Struct object.
     */

    @Override
    public Struct createStruct(final String typeName, final Object[] attributes) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Returns true if this connection is still valid.
     * 
     * @param timeout
     *            the number of seconds to wait for the database to respond (ignored)
     * @return true if the connection is valid.
     */
    @Override
    public synchronized boolean isValid(final int timeout) {

        try {
            debugCodeCall("isValid", timeout);
            if (session == null || session.isClosed()) { return false; }
            // force a network round trip (if networked)
            getInternalAutoCommit();
            return true;
        }
        catch (final Exception e) {
            // this method doesn't throw an exception, but it logs it
            logAndConvert(e);
            return false;
        }
    }

    /**
     * [Not supported] Set a client property.
     */

    @Override
    public void setClientInfo(final String name, final String value) throws SQLClientInfoException {

        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Set the client properties.
     */

    @Override
    public void setClientInfo(final Properties properties) throws SQLClientInfoException {

        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Get the client properties.
     */

    @Override
    public Properties getClientInfo() throws SQLClientInfoException {

        throw new SQLClientInfoException();
    }

    /**
     * [Not supported] Set a client property.
     */

    @Override
    public String getClientInfo(final String name) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Return an object of this class if possible.
     * 
     * @param iface
     *            the class
     */

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     * 
     * @param iface
     *            the class
     */

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {

        throw Message.getUnsupportedException();
    }

    /**
     * Create a Clob value from this reader.
     * 
     * @param x
     *            the reader
     * @param length
     *            the length (if smaller or equal than 0, all data until the end of file is read)
     * @return the value
     */
    public Value createClob(final Reader x, long length) throws SQLException {

        if (x == null) { return ValueNull.INSTANCE; }
        if (length <= 0) {
            length = -1;
        }
        final Value v = ValueLob.createClob(x, length, session.getDataHandler());
        return v;
    }

    /**
     * Create a Blob value from this input stream.
     * 
     * @param x
     *            the input stream
     * @param length
     *            the length (if smaller or equal than 0, all data until the end of file is read)
     * @return the value
     */
    public Value createBlob(final InputStream x, long length) throws SQLException {

        if (x == null) { return ValueNull.INSTANCE; }
        if (length <= 0) {
            length = -1;
        }
        final Value v = ValueLob.createBlob(x, length, session.getDataHandler());
        return v;
    }

    private void checkMap(final Map map) throws SQLException {

        if (map != null && map.size() > 0) { throw Message.getUnsupportedException(); }
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {

        return getTraceObjectName() + ": url=" + url + " user=" + user;
    }

    /**
     * Convert an object to the default Java object for the given SQL type. For example, LOB objects are converted to java.sql.Clob /
     * java.sql.Blob.
     * 
     * @param v
     *            the value
     * @return the object
     */
    Object convertToDefaultObject(final Value v) throws SQLException {

        Object o;
        switch (v.getType()) {
            case Value.CLOB: {
                if (SysProperties.RETURN_LOB_OBJECTS) {
                    final int id = getNextId(TraceObject.CLOB);
                    o = new JdbcClob(this, v, id);
                }
                else {
                    o = v.getObject();
                }
                break;
            }
            case Value.BLOB: {
                if (SysProperties.RETURN_LOB_OBJECTS) {
                    final int id = getNextId(TraceObject.BLOB);
                    o = new JdbcBlob(this, v, id);
                }
                else {
                    o = v.getObject();
                }
                break;
            }
            case Value.JAVA_OBJECT:
                if (Constants.SERIALIZE_JAVA_OBJECTS) {
                    o = ObjectUtils.deserialize(v.getBytesNoCopy());
                }
                else {
                    o = v.getObject();
                }
                break;
            default:
                o = v.getObject();
        }
        return o;
    }

    @Override
    public void abort(final Executor arg0) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public String getSchema() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void setNetworkTimeout(final Executor arg0, final int arg1) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public void setSchema(final String arg0) throws SQLException {

        throw Message.getUnsupportedException();
    }

}
