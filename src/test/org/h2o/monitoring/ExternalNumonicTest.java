package org.h2o.monitoring;

import java.io.IOException;

import org.h2o.autonomic.numonic.NumonicReporter;
import org.h2o.autonomic.numonic.Threshold;
import org.h2o.autonomic.numonic.ThresholdChecker;

public class ExternalNumonicTest {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(final String[] args) throws IOException {

        final Threshold[] thresholds = ThresholdChecker.getThresholds("thresholds.properties");

        final NumonicReporter reporter = new NumonicReporter("numonic.properties", thresholds);

        reporter.start();
    }
}
