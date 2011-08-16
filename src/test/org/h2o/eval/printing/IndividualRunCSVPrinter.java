package org.h2o.eval.printing;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.h2o.eval.script.workload.FailureLogEntry;
import org.h2o.eval.script.workload.QueryLogEntry;
import org.h2o.eval.script.workload.WorkloadResult;

import uk.ac.standrews.cs.nds.util.FileUtil;

/**
 * Utility class to print results of each co-ordinator script execution to a new CSV file.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class IndividualRunCSVPrinter extends Printer {

    /**
     * 
     * @param fileLocation The file results are to be written to.
     * @param workloadResults The results to be written.
     * @throws IOException 
     */
    public static void printResults(final String fileLocation, final List<WorkloadResult> workloadResults, final List<FailureLogEntry> failureLog) throws IOException {

        final Set<String> tableNames = getAllTableNames(workloadResults);

        final long startTime = getStartTime(workloadResults);

        final StringBuilder csv = new StringBuilder();

        //  csv.append(printBasicWorkloadDetails(workloadResults));

        csv.append(QueryLogEntry.toCSVHeader(tableNames));

        for (final WorkloadResult workloadResult : workloadResults) {

            if (workloadResult.getQueryLog() != null) {

                csv.append(printQueryLog(workloadResult.getQueryLog(), workloadResult.getLocationOfExecution(), tableNames, startTime));
            }
        }

        csv.append(printFailureLog(failureLog, startTime));

        FileUtil.writeToFile(fileLocation, csv.toString(), true);
    }

    public static StringBuilder printQueryLog(final List<QueryLogEntry> queryLogList, final String location, final Collection<String> tableNames, final long startTime) {

        final StringBuilder csv = new StringBuilder();

        for (final QueryLogEntry queryLog : queryLogList) {
            csv.append(queryLog.toCSV(dateFormatter, location, tableNames, startTime));
        }

        return csv;
    }

    private static StringBuilder printFailureLog(final List<FailureLogEntry> failureLog, final long startTime) {

        final StringBuilder csv = new StringBuilder();

        for (final FailureLogEntry queryLog : failureLog) {
            csv.append(queryLog.toCSV(dateFormatter, startTime));
        }

        return csv;
    }

}
