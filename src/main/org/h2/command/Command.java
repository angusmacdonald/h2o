/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.util.MemoryUtils;
import org.h2.util.ObjectArray;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * Represents a SQL statement. This object is only used on the server side.
 */
public abstract class Command implements CommandInterface {

    /**
     * The session.
     */
    protected final Session session;

    /**
     * The trace module.
     */
    protected final Trace trace;

    /**
     * The last start time.
     */
    protected long startTime;

    /**
     * If this query was canceled.
     */
    private volatile boolean cancel;

    private String sql;

    public Command(final Parser parser, final String sql) {

        session = parser.getSession();
        this.sql = sql;
        trace = session.getDatabase().getTrace(Trace.COMMAND);
    }

    /**
     * Check if this command is transactional. If it is not, then it forces the current transaction to commit.
     * 
     * @return true if it is
     */
    public abstract boolean isTransactional();

    /**
     * Check if this command is a query.
     * 
     * @return true if it is
     */
    @Override
    public abstract boolean isQuery();

    /**
     * Get the list of parameters.
     * 
     * @return the list of parameters
     */
    @Override
    public abstract ObjectArray getParameters();

    /**
     * Check if this command is read only.
     * 
     * @return true if it is
     */
    public abstract boolean isReadOnly();

    /**
     * Get an empty result set containing the meta data.
     * 
     * @return an empty result set
     */
    public abstract LocalResult queryMeta() throws SQLException;

    /**
     * Execute an updating statement, if this is possible.
     * 
     * @return the update count
     * @throws SQLException
     *             if the command is not an updating statement
     * @throws RPCException
     */
    public int update() throws SQLException, RPCException {

        throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Local. Won't commit if it is part of a bigger transaction.
     * 
     * Execute an updating statement, if this is possible.
     * 
     * @return the update count
     * @throws SQLException
     *             if the command is not an updating statement
     * @throws RPCException
     */
    protected int update(final boolean partOfABiggerThing) throws SQLException, RPCException {

        throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute a query statement, if this is possible.
     * 
     * @param maxrows
     *            the maximum number of rows returned
     * @return the local result set
     * @throws SQLException
     *             if the command is not a query
     * @throws RPCException
     */
    public LocalResult query(final int maxrows) throws SQLException, RPCException {

        throw Message.getSQLException(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Local. Won't commit if it is part of a bigger transaction.
     * 
     * @param maxrows
     *            the maximum number of rows returned
     * @return the local result set
     * @throws SQLException
     *             if the command is not a query
     */
    protected LocalResult query(final int maxrows, final boolean partOfABiggerThing) throws SQLException {

        throw Message.getSQLException(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    public final LocalResult getMetaDataLocal() throws SQLException {

        return queryMeta();
    }

    @Override
    public final ResultInterface getMetaData() throws SQLException {

        return queryMeta();
    }

    @Override
    public ResultInterface executeQuery(final int maxrows, final boolean scrollable) throws SQLException {

        return executeQueryLocal(maxrows);
    }

    /**
     * Execute a query and return a local result set. This method prepares everything and calls {@link #query(int)} finally.
     * 
     * @param maxrows
     *            the maximum number of rows to return
     * @return the local result set
     */
    public LocalResult executeQueryLocal(final int maxrows) throws SQLException {

        startTime = System.currentTimeMillis();
        final Database database = session.getDatabase();
        session.waitIfExclusiveModeEnabled();

        synchronized (session) {

            try {
                database.checkPowerOff();
                session.setCurrentCommand(this, startTime);
                return query(maxrows);
            }
            catch (final Exception e) {
                e.printStackTrace();
                final SQLException s = Message.convert(e, sql);
                database.exceptionThrown(s, sql);
                throw s;
            }
            finally {
                stop();
            }
        }
    }

    /**
     * Start the stopwatch.
     */
    void start() {

        startTime = System.currentTimeMillis();
    }

    /**
     * Check if this command has been canceled, and throw an exception if yes.
     * 
     * @throws SQLException
     *             if the statement has been canceled
     */
    public void checkCanceled() throws SQLException {

        if (cancel) {
            cancel = false;
            throw Message.getSQLException(ErrorCode.STATEMENT_WAS_CANCELED);
        }
    }

    private void stop() throws SQLException {

        session.closeTemporaryResults();
        session.setCurrentCommand(null, 0);
        if (!isTransactional()) {
            session.commit(true);
        }
        else {
            final Database db = session.getDatabase();
            if (db != null) {
                if (db.getLockMode() == Constants.LOCK_MODE_READ_COMMITTED) {
                    session.unlockH2ReadLocks();
                }
            }
        }
        if (trace.isInfoEnabled()) {
            final long time = System.currentTimeMillis() - startTime;
            if (time > Constants.SLOW_QUERY_LIMIT_MS) {
                trace.info("slow query: " + time);
            }
        }
    }

    @Override
    public int executeUpdate(final boolean partOfMultiQueryTransaction) throws SQLException {

        final long start = startTime = System.currentTimeMillis();
        final Database database = session.getDatabase();

        session.waitIfExclusiveModeEnabled();

        final int rollback = session.getLogId();
        session.setCurrentCommand(this, startTime);
        try {
            while (true) {
                database.checkPowerOff();
                try {
                    return update(partOfMultiQueryTransaction);
                }
                catch (final OutOfMemoryError e) {
                    MemoryUtils.freeReserveMemory();
                    throw Message.convert(e);
                }
                catch (final SQLException e) {
                    if (e.getErrorCode() == ErrorCode.CONCURRENT_UPDATE_1) {
                        final long now = System.currentTimeMillis();
                        if (now - start > session.getLockTimeout()) { throw e; }
                        try {
                            Thread.sleep(100);
                        }
                        catch (final InterruptedException e1) {
                            // ignore
                        }
                        continue;
                    }
                    throw e;
                }
                catch (final Throwable e) {
                    e.printStackTrace();
                    throw Message.convert(e);
                }
            }
        }
        catch (final SQLException e) {

            Message.addSQL(e, sql);
            database.exceptionThrown(e, sql);
            database.checkPowerOff();
            if (e.getErrorCode() == ErrorCode.DEADLOCK_1) {
                session.rollback();
            }
            else if (e.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                // try to rollback, saving memory
                try {
                    session.rollbackTo(rollback, true);
                }
                catch (final SQLException e2) {
                    if (e2.getErrorCode() == ErrorCode.OUT_OF_MEMORY) {
                        // if rollback didn't work, there is a serious problem:
                        // the transaction may be applied partially
                        // in this case we need to panic:
                        // close the database
                        session.getDatabase().shutdownImmediately();
                    }
                    throw e2;
                }
            }
            else {
                session.rollbackTo(rollback, false);
            }
            throw e;
        }
        finally {
            stop();
        }
    }

    @Override
    public void close() {

        sql = null;
    }

    @Override
    public void cancel() {

        cancel = true;
    }

    @Override
    public String toString() {

        return TraceObject.toString(sql, getParameters());
    }

    /**
     * Request a query proxy from the Table Manager of the table involved in the query. This proxy contains the details of any locks that
     * were acquired.
     * 
     * @return
     * @throws SQLException
     */
    public abstract void acquireLocks() throws SQLException;

    /**
     * @return the session
     */
    public Session getSession() {

        return session;
    }

    /**
     * @return
     */
    public abstract boolean shouldBePropagated();

    /**
     * Is the given command meant to be propagated to all machines?
     * 
     * @param addNewReplicaLocationQuery
     * @return
     */
    protected boolean isPropagatableCommand(final Command command) {

        return command.shouldBePropagated();
    }

    /**
     * Get the SQL string for the given query, including parameter values. This differs from
     * {@link #getSQL()} because prepared statement queries show only question marks where parameters should be.
     * This method replaces those question marks with the actual values used in the query.
     * @return Query string for a single query.
     */
    public String getSQLIncludingParameters() {

        return toString();
    }
}
