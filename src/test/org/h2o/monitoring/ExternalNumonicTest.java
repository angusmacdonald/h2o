package org.h2o.monitoring;

import org.h2o.autonomic.numonic.NumonicReporter;
import org.h2o.autonomic.numonic.Threshold;

import uk.ac.standrews.cs.numonic.appinterface.ResourceType;

public class ExternalNumonicTest {

    /**
     * @param args
     */
    public static void main(final String[] args) {

        final Threshold testThreshold = new Threshold(ResourceType.CPU_USER, 0.5d, true);

        final NumonicReporter reporter = new NumonicReporter("numonic.properties", testThreshold);

        reporter.start();
    }
}
