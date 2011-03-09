/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.sql.SQLException;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2o.db.id.TableInfo;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;
import org.h2o.test.fixture.H2OTest;

import uk.ac.standrews.cs.nds.rpc.RPCException;

public class RemoteQueryExecutor extends Thread {

    private final String query;

    private final String transactionName;

    private final DatabaseInstanceWrapper databaseWrapper;

    private final boolean local;

    private final Parser parser;

    private final boolean commitOperation;

    private final int updateID;

    private final TableInfo tableInfo;

    /**
     * 
     * @param query
     * @param transactionName
     * @param replica
     * @param updateID
     *            The current update ID of the table involved in this query.
     * @param parser
     *            Only used to execute the transaction if local is true.
     * @param local
     *            Whether the transaction is to be executed locally, or sent remotely.
     * @param commitOperation
     *            True if this is a COMMIT, false if it is another type of query. If it is false a PREPARE command will be executed to get
     *            ready for the eventual commit.
     */
    public RemoteQueryExecutor(final String query, final String transactionName, final DatabaseInstanceWrapper replica, final int updateID, final Parser parser, final boolean local, final boolean commitOperation, final TableInfo tableInfo) {

        this.query = query;
        this.transactionName = transactionName;
        databaseWrapper = replica;
        this.parser = parser;
        this.local = local;
        this.commitOperation = commitOperation;
        this.updateID = updateID;
        this.tableInfo = tableInfo;
    }

    public QueryResult executeQuery() {

        if (local) {
            return executeLocal();
        }
        else {
            return executeRemote();
        }
    }

    private QueryResult executeLocal() {

        QueryResult qr = null;
        try {
            int result = 0;

            if (!commitOperation) {

                // Execute query.
                Command command = parser.prepareCommand(query);
                result = command.executeUpdate(true); // True because it may need to wait
                // for the remote machine to
                // commit.

                // Prepare query for commit.
                command = parser.prepareCommand("PREPARE COMMIT " + transactionName);
                final int prepareResult = command.executeUpdate(); //XXX efficiency improvement: piggyback the prepare on the initial query call.

                if (prepareResult != 0) {
                    result = -1; //signals to the requesting instance that there was a failure preparing the command.
                }
            }
            else {
                // Prepare for commit.
                final Command prepare = parser.prepareCommand("PREPARE COMMIT " + transactionName);
                prepare.executeUpdate();

                // Execute query.
                final Command command = parser.prepareCommand(query);
                result = command.executeUpdate();
            }

            qr = new QueryResult(result, databaseWrapper, updateID, tableInfo);

        }
        catch (final SQLException e) {
            qr = new QueryResult(e, databaseWrapper, updateID, tableInfo);
        }

        return qr;
    }

    private QueryResult executeRemote() {

        QueryResult qr = null;
        try {
            H2OTest.rmiFailure(databaseWrapper);

            assert databaseWrapper != null && databaseWrapper.getDatabaseInstance() != null : "The wrapper supplied for remote query should not contain a null RMI reference.";

            final int result = databaseWrapper.getDatabaseInstance().execute(query, transactionName, commitOperation);

            qr = new QueryResult(result, databaseWrapper, updateID, tableInfo);

        }
        catch (final RPCException e) {
            e.printStackTrace();
            qr = new QueryResult(new SQLException(e.getMessage()), databaseWrapper, updateID, tableInfo);
        }
        catch (final SQLException e) {
            qr = new QueryResult(e, databaseWrapper, updateID, tableInfo);
        }

        return qr;
    }
}
