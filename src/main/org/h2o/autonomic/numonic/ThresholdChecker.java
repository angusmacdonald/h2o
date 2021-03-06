package org.h2o.autonomic.numonic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.Set;

import org.h2o.autonomic.numonic.threshold.Threshold;
import org.h2o.util.H2OPropertiesWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.numonic.data.Data;
import uk.ac.standrews.cs.numonic.data.ResourceType;
import uk.ac.standrews.cs.numonic.summary.MeasurementType;
import uk.ac.standrews.cs.numonic.summary.SingleSummary;
import uk.ac.standrews.cs.numonic.util.IPropertiesWrapper;
import uk.ac.standrews.cs.numonic.util.JarPropertiesWrapper;

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
    public static Threshold[] getThresholds(final IPropertiesWrapper thresholdProperties) {

        final Set<Object> resourceNames = thresholdProperties.getKeys();

        final List<Threshold> parsedThresholds = new ArrayList<Threshold>();

        /*
         * Loop through every resource listed in the thresholds property file and generate threshold objects for each one that 
         * a threshold is specified for.
         */
        for (final Object resourceName : resourceNames) { //for every property of the format: 'cpu_user_total = 0.5d, true'

            final String value = thresholdProperties.getProperty((String) resourceName);

            if (value.equals("ignore")) {
                continue;
            }

            final String[] thresholdInfo = value.split(",");

            if (thresholdInfo.length != 2) {
                ErrorHandling.errorNoEvent("Threshold data in properties file is in an incorrect format: " + value + " (expected <value>, [above | below])");
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

        if (thresholdPropertiesLocation.startsWith("JAR:")) {
            //Get the properties file from inside the H2O jar file (note: this will still work in eclipse, etc.)

            final String location = thresholdPropertiesLocation.substring("JAR:".length());

            final File f = new File(location);

            if (f.exists()) {
                return getPropertiesFromFile(location);
            }
            else { //get from jar.

                final PropertiesFileLoader loader = new PropertiesFileLoader();

                final IPropertiesWrapper propertiesWrapper = new JarPropertiesWrapper(loader.getResource(location));
                propertiesWrapper.loadProperties();

                return getThresholds(propertiesWrapper);
            }
        }
        else {

            return getPropertiesFromFile(thresholdPropertiesLocation);

        }
    }

    public static Threshold[] getPropertiesFromFile(final String thresholdPropertiesLocation) throws IOException {

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
