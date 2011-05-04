package org.h2o.autonomic.numonic;

import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.appinterface.threshold.MeasurementType;
import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

public class ThresholdChecker {

    /**
     * Thresholds to monitor.
     */
    private final Threshold[] thresholds;

    /**
     * Instantiate class with the collection of thresholds that should be monitored.
     * @param thresholds2
     */
    public ThresholdChecker(final Threshold[] thresholds) {

        this.thresholds = thresholds;

    }

    public void analyseNewMonitoringData(final SingleSummary<? extends Data> summary) {

        final Data averageOfMonitoringData = summary.get(MeasurementType.AVG);

        for (final Threshold threshold : thresholds) {
            //For each threshold check against the monitored value, and raise an event if the threshold is exceeded.
            try {
                final double monitoredValue = averageOfMonitoringData.getDouble(threshold.resourceName);

                if (threshold.above && monitoredValue > threshold.value) {
                    System.out.println("Threshold Exceeded: " + monitoredValue + " > " + threshold.value);
                    //do something.
                }
                else if (!threshold.above && monitoredValue < threshold.value) {
                    System.out.println("Threshold Breached: " + monitoredValue + " < " + threshold.value);
                    //do something.
                }
                else {
                    System.out.println("Threshold (" + threshold.value + ") not exceeded (" + monitoredValue + ")");
                }
            }
            catch (final NoSuchFieldException e) {
                ErrorHandling.exceptionErrorNoEvent(e, "Error reading threshold information. The field for one of the resources being monitored could not be found.");
            }
        }

    }

}
