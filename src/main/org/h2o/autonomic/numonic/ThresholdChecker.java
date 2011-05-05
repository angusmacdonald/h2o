package org.h2o.autonomic.numonic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Set;

import org.h2o.util.H2OPropertiesWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.appinterface.ResourceType;
import uk.ac.standrews.cs.numonic.appinterface.threshold.MeasurementType;
import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;

public class ThresholdChecker extends Observable {

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
                    Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Threshold for '" + threshold.resourceName + "' has been crossed: " + monitoredValue + " > " + threshold.value);

                    setChanged();
                    notifyObservers(threshold);
                }
                else if (!threshold.above && monitoredValue < threshold.value) {
                    Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Threshold for '" + threshold.resourceName + "' has been crossed: " + monitoredValue + " < " + threshold.value);

                    setChanged();
                    notifyObservers(threshold);

                }
                else {
                    Diagnostic.traceNoEvent(DiagnosticLevel.INIT, ": " + monitoredValue + " < " + threshold.value);

                }
            }
            catch (final NoSuchFieldException e) {
                //Will occur.
                //XXX find more effective way of doing this if it is too inefficient to throw an exception each time.
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

        /*
         * Loop through every resource listed in the thresholds property file and generate threshold objects for each one that 
         * a threshold is specified for.
         */
        for (final Object resourceName : resourceNames) { //for every property of the format: 'cpu_user_total = 0.5d, true'

            final String value = thresholdProperties.getProperty((String) resourceName);

            System.out.println(resourceName);

            if (value.equals("ignore")) {
                continue;
            }

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

    /**
     * Takes a threshold that is typed as object and casts it to Threshold. Called by observer classes from
     * their update method.
     * @param arg typed as Object, but is in fact Threshold.
     * @return
     */
    public static Threshold getThresholdObject(final Object arg) {

        return (Threshold) arg;
    }

}
