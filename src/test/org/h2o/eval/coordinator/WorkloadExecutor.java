package org.h2o.eval.coordinator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.worker.WorkloadResult;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Parses workloads and executes them against given database connections.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class WorkloadExecutor {

    private static final String COMMENT = "#";
    private static final String SLEEP_OPEN_TAG = "<sleep>";
    private static final String SLEEP_CLOSE_TAG = "</sleep>";

    private static final String LOOP_START_OPEN_TAG = "<loop";
    private static final String LOOP_COUNTER_PLACEHOLDER = "<loop-counter/>";
    private static final String LOOP_END_TAG = "</loop>";

    public static WorkloadResult execute(final Connection connection, final ArrayList<String> queries, final IWorker worker) throws SQLException, WorkloadParseException {

        final Statement stat = connection.createStatement();

        long successfullyExecutedTransactions = 0;
        long attemptedTransactions = 0;

        int loopCounter = -1; //the current iteration of the loop in this workload [nested loops are not supported].
        int loopStartPos = -1; //where the loop starts in this list of queries.
        int loopIterations = 1; //how many iterations of the loop are to be executed.

        for (int i = 0; i < queries.size(); i++) {

            String query = queries.get(i);

            if (query.startsWith(COMMENT)) {
                continue; //it's a comment.
            }
            else if (query.startsWith(SLEEP_OPEN_TAG)) { //Sleep for a specified number of seconds.

                try {
                    final int sleepTime = Integer.valueOf(query.substring(SLEEP_OPEN_TAG.length(), query.indexOf(SLEEP_CLOSE_TAG)));
                    try {
                        Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Sleeping for " + sleepTime + " seconds.");
                        Thread.sleep(sleepTime);
                    }
                    catch (final InterruptedException e) {
                    }
                }
                catch (final NumberFormatException e) {
                    throw new WorkloadParseException("Incorrectly formatted workload request: " + query);
                }
            }
            else if (query.startsWith(LOOP_START_OPEN_TAG)) { //We have reached the start of a loop.

                try {
                    loopIterations = Integer.valueOf(query.substring(query.indexOf("iterations=\"") + "iterations=\"".length(), query.lastIndexOf("\">")));
                }
                catch (final Exception e) {
                    throw new WorkloadParseException("Incorrectly formatted workload request: " + query + " (exception: " + e.getMessage() + ").");
                }

                if (loopIterations < 0) { throw new WorkloadParseException("The number of loop iterations must be positive."); }

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Starting loop for " + loopIterations + " iterations.");
                loopCounter = 0;
                loopStartPos = i;
                continue;
            }
            else if (query.startsWith(LOOP_END_TAG)) { //We have reached the end of the loop.

                if (loopCounter == loopIterations) {
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Ending loop at " + loopCounter + ".");
                    continue; //break out of loop
                }
                else {
                    i = loopStartPos;
                    loopCounter++;
                    continue;
                }
            }
            else { //It's an SQL query. Execute it.

                attemptedTransactions++;

                query = query.replaceAll(LOOP_COUNTER_PLACEHOLDER, loopCounter + "");

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Executing query: " + query);

                try {
                    stat.execute(query);
                    successfullyExecutedTransactions++;
                }
                catch (final SQLException e) {
                    Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to execute '" + query + "'. Error: " + e.getMessage());
                }
            }

        }

        final WorkloadResult result = new WorkloadResult(successfullyExecutedTransactions, attemptedTransactions, worker);
        return result;

    }
}
