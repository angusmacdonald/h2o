/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command;

import java.sql.SQLException;

import org.h2.result.LocalResult;
import org.h2.util.ObjectArray;
import org.h2o.db.query.TableProxyManager;
import org.h2o.test.fixture.H2OTest;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * Represents a list of SQL statements.
 */
public class CommandList extends Command {

    private final Command command;

    private String[] remaining;

    // TODO lock if possible!

    public CommandList(final Parser parser, final String sql, final Command c, final String remaining) {

        super(parser, sql);
        command = c;

        /*
         * Split and store remaining commands.
         */
        if (remaining != null) {
            this.remaining = remaining.split(";"); // TODO not particularly safe. i.e. no query can contain a semi-colon.
        }
    }

    @Override
    public ObjectArray getParameters() {

        return command.getParameters();
    }

    @Override
    public int executeUpdate() throws SQLException {

        return executeUpdate(true);
    }

    private SQLException executeRemaining() throws SQLException, RPCException {

        SQLException rollbackException = null;

        if (remaining != null) {
            try {
                /*
                 * H2O. Iterate through remaining commands rather than recursively calling this method.
                 */
                for (final String sqlStatement : remaining) {

                    final Command remainingCommand = session.prepareLocal(sqlStatement);

                    if (remainingCommand.isQuery()) {
                        remainingCommand.query(0, true);
                    }
                    else {
                        remainingCommand.update(true);
                    }
                }

                H2OTest.queryFailure();

            }
            catch (final SQLException e) {
                rollbackException = e;
            }
        }

        return rollbackException;
    }

    @Override
    protected int update(final boolean partOfMultiQueryTransaction) throws SQLException, RPCException {

        return update();
    }

    @Override
    public int update() throws SQLException, RPCException {

        /*
         * Execute the first update, then iterate through every subsequent update.
         */

        final int updateCount = command.executeUpdate(true);
        final SQLException rollbackException = executeRemaining();

        commit(rollbackException);

        return updateCount;
    }

    @Override
    public LocalResult query(final int maxrows) throws SQLException, RPCException {

        final LocalResult result = command.query(maxrows);
        final SQLException rollbackException = executeRemaining();

        commit(rollbackException);

        return result;
    }

    /**
     * Commit or rollback a transaction based on whether an exception was thrown during the update/query.
     * 
     * @param rollbackException
     *            Exception thrown during query. Will be null if none was thrown and transaction was successful.
     * @throws SQLException
     */
    private void commit(final SQLException rollbackException) throws SQLException {

        if (session.getApplicationAutoCommit() || rollbackException != null) {
            /*
             * Having executed all commands, rollback if there was an exception. Otherwise, commit.
             */

            final TableProxyManager currentProxyManager = session.getProxyManagerForTransaction();
            currentProxyManager.finishTransaction(rollbackException == null, true, session.getDatabase());

            /*
             * If we did a rollback, rethrow the exception that caused this to happen.
             */
            if (rollbackException != null) { throw rollbackException; }
        }
    }

    @Override
    public boolean isQuery() {

        return command.isQuery();
    }

    @Override
    public boolean isTransactional() {

        return true;
    }

    @Override
    public boolean isReadOnly() {

        return false;
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        return command.queryMeta();
    }

    @Override
    public void acquireLocks() throws SQLException {

        command.acquireLocks();
    }

    @Override
    public boolean shouldBePropagated() {

        return command.shouldBePropagated();
    }

    @Override
    public void setIsPreparedStatement(final boolean preparedStatement) {

        command.setIsPreparedStatement(preparedStatement);
    }
}
