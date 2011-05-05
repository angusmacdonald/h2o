package org.h2o.autonomic.numonic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.h2o.util.H2OPropertiesWrapper;

import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.appinterface.ResourceType;
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

    /**
     * Read in threshold settings from a properties file and return them as an array of threshold objects.
     * @param thresholdProperties The properties file which contains threshold information.
     * @return
     */
    public static Threshold[] getThresholds(final H2OPropertiesWrapper thresholdProperties) {

        final Set<Object> resourceNames = thresholdProperties.getKeys();

        final List<Threshold> parsedThresholds = new ArrayList<Threshold>();

        for (final Object resourceName : resourceNames) { //for every property of the format: 'cpu_user_total = 0.5d, true'

            final String value = thresholdProperties.getProperty((String) resourceName);

            if (value.equals("ignore")) {
                continue;
            }

            System.out.println(resourceName + ": " + value);

            final String[] thresholdInfo = value.split(",");

            if (thresholdInfo.length != 2) {
                ErrorHandling.errorNoEvent("Threshold data in file '" + thresholdProperties.getLocation() + "' is in an incorrect format: " + value + " (expected <value>, [above | below])");
                continue;
            }

            final ResourceType resourceBeingMonitored = ResourceType.get(resourceName.toString());
            final double thresholdValue = Double.parseDouble(thresholdInfo[0].trim());
            final boolean aboveOrBelow = Boolean.parseBoolean(thresholdInfo[1].trim());

            final Threshold newThreshold = new Threshold(resourceBeingMonitored, thresholdValue, aboveOrBelow);

            parsedThresholds.add(newThreshold);

        }

        return parsedThresholds.toArray(new Threshold[0]);
    }

    public static Threshold[] getThresholds(final String thresholdPropertiesLocation) throws IOException {

        final H2OPropertiesWrapper propertiesFile = H2OPropertiesWrapper.getWrapper(thresholdPropertiesLocation);
        propertiesFile.loadProperties();
        return getThresholds(propertiesFile);
    }

}
