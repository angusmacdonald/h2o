package org.h2o.eval.printing;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.h2o.eval.script.workload.WorkloadResult;

import uk.ac.standrews.cs.nds.util.FileUtil;

/**
 * Utility class to print the results of executing a workload in time slices.
 *
 * @author Angus Macdonald (angus.macdonald@st-andrews.ac.uk)
 */
public class TimeSlicePrinter extends Printer {

    /**
     * 
     * @param fileLocation The file results are to be written to.
     * @param workloadResults The results to be written.
     * @throws IOException 
     */
    public static void printResults(final String fileLocation, final List<WorkloadResult> workloadResults, final long workloadLength, final int timeSlicePeriodSecs) throws IOException {

        final int numberOfTimeSlices = (int) (workloadLength / (timeSlicePeriodSecs * 1000));

        if (!new File(fileLocation).exists()) {
            FileUtil.writeToFile(fileLocation, printHeader(numberOfTimeSlices, timeSlicePeriodSecs), true);
        }

        FileUtil.writeToFile(fileLocation, printRow(getNumberOfSuccessfulTransactions(workloadResults, numberOfTimeSlices, timeSlicePeriodSecs)), false);
    }

    private static String printHeader(final int numberOfTimeSlices, final int timeSlicePeriod) {

        StringBuilder row = new StringBuilder();

        long currentTime = 0;

        for (int i = 0; i < numberOfTimeSlices; i++) {
            final long newTime = currentTime + timeSlicePeriod;
            row = appendToRow(row, i != 0, currentTime + "-" + newTime + "s");

            currentTime = newTime;

        }

        row = endRow(row);
        return row.toString();
    }

    protected static long[] getNumberOfSuccessfulTransactions(final List<WorkloadResult> workloadResults, final int numberOfTimeSlices, final int timeSlicePeriod) {

        final long[] throughputPerTimeSlicePerSecond = new long[numberOfTimeSlices];

        for (final WorkloadResult workloadResult : workloadResults) {

            long currentTime = 0;

            for (int i = 0; i < numberOfTimeSlices; i++) {
                final long newTime = currentTime + timeSlicePeriod;

                final long timeSliceThroughput = workloadResult.getNumberOfSuccessfulTransactionsBetween(currentTime * 1000, newTime * 1000);

                throughputPerTimeSlicePerSecond[i] = timeSliceThroughput / timeSlicePeriod;

                currentTime = newTime;
            }
        }

        return throughputPerTimeSlicePerSecond;
    }

    private static String printRow(final long[] numberOfSuccessfulTransactions) {

        StringBuilder row = new StringBuilder();

        for (int i = 0; i < numberOfSuccessfulTransactions.length; i++) {
            row = appendToRow(row, i != 0, numberOfSuccessfulTransactions[i] + "");
        }

        row = endRow(row);
        return row.toString();
    }

    private static StringBuilder appendToRow(final StringBuilder row, final boolean addComma, final Object obj) {

        return row.append((addComma ? "," : "") + obj);

    }

    private static StringBuilder endRow(final StringBuilder row) {

        return row.append("\n");
    }

}
