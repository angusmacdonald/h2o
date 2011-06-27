package org.h2o.eval.printing;

import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.h2o.eval.script.workload.QueryLogEntry;
import org.h2o.eval.script.workload.WorkloadResult;

import uk.ac.standrews.cs.nds.util.FileUtil;

/**
 * Utility class to print results to a CSV file.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class CSVPrinter {

    private static final DateFormat dateFormatter = new SimpleDateFormat("dd MMM yyyy hh:mm:ss");

    /**
     * 
     * @param fileLocation The file results are to be written to.
     * @param workloadResults The results to be written.
     * @throws FileNotFoundException If the file could not be created or written to.
     */
    public static void printResults(final String fileLocation, final List<WorkloadResult> workloadResults) throws FileNotFoundException {

        final StringBuilder csv = new StringBuilder();

        //  csv.append(printBasicWorkloadDetails(workloadResults));

        csv.append(QueryLogEntry.toCSVHeader());

        for (final WorkloadResult workloadResult : workloadResults) {

            csv.append(printQueryLog(workloadResult.getQueryLog(), workloadResult.getLocationOfExecution()));
        }

        FileUtil.writeToFile(fileLocation, csv.toString());
    }

    private static StringBuilder printBasicWorkloadDetails(final WorkloadResult workloadResult) {

        final StringBuilder csv = new StringBuilder();

        csv.append("Evaluation results:\n");
        csv.append("Total attempted transactions:, " + workloadResult.getTotalAttemptedTransactions() + "\n");
        csv.append("Total successful transactions:, " + workloadResult.getTotalSuccessfulTransactions() + "\n\n");

        return csv;
    }

    public static StringBuilder printQueryLog(final List<QueryLogEntry> queryLogList, final String location) {

        final StringBuilder csv = new StringBuilder();

        for (final QueryLogEntry queryLog : queryLogList) {
            csv.append(queryLog.toCSV(dateFormatter, location));
        }

        return csv;
    }
}