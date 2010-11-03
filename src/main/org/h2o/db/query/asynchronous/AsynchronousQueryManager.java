/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.FutureTask;

import org.h2.engine.Database;
import org.h2o.db.id.TableInfo;

/**
 * Manages the set of updates currently being executed by this database instance. There is one instance of this class per database instance.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public final class AsynchronousQueryManager {

    private final AsynchronousQueryCheckerThread checkerThread = new AsynchronousQueryCheckerThread(this);

    private final Database db;

    public AsynchronousQueryManager(final Database db) {

        this.db = db;
        checkerThread.start();
    }

    /**
     * Map of currently executing transactions.
     * 
     * <p>
     * Key: transaction name
     * <p>
     * Value: transaction object
     */
    Map<String, Transaction> activeTransactions = new HashMap<String, Transaction>();

    public synchronized void addTransaction(final String transactionNameForQuery, final TableInfo tableName, final List<FutureTask<QueryResult>> incompleteQueries, final List<CommitResult> recentlyCompletedQueries, final int expectedUpdateID) {

        if (activeTransactions.containsKey(transactionNameForQuery)) {
            final Transaction existingTransaction = activeTransactions.get(transactionNameForQuery);
            existingTransaction.addQueries(incompleteQueries);
            existingTransaction.addCompletedQueries(recentlyCompletedQueries);
            activeTransactions.put(transactionNameForQuery, existingTransaction);
        }
        else {
            final Transaction newTransaction = new Transaction(transactionNameForQuery, incompleteQueries, recentlyCompletedQueries, expectedUpdateID);

            activeTransactions.put(transactionNameForQuery, newTransaction);
        }
    }

    public synchronized Transaction getTransaction(final String transactionID) {

        return activeTransactions.get(transactionID);
    }

    /**
     * Go through all active transactions and check if any have finished executing.
     */
    public synchronized void checkForCompletion() {

        final List<String> finishedTransactions = new LinkedList<String>();

        for (final Entry<String, Transaction> activeTransaction : activeTransactions.entrySet()) {
            final boolean finished = activeTransaction.getValue().checkForCompletion(db);

            if (finished) {
                finishedTransactions.add(activeTransaction.getKey());
            }
        }

        for (final String transactionName : finishedTransactions) {
            activeTransactions.remove(transactionName);
        }
    }

}
