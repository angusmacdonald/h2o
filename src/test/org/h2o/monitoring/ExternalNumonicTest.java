package org.h2o.monitoring;

import java.io.IOException;
import java.util.Observable;
import java.util.Observer;

import org.h2o.autonomic.numonic.NumonicReporter;
import org.h2o.autonomic.numonic.Threshold;
import org.h2o.autonomic.numonic.ThresholdChecker;

public class ExternalNumonicTest implements Observer {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(final String[] args) throws IOException {

        final ExternalNumonicTest test = new ExternalNumonicTest();
        test.start();
    }

    public void start() throws IOException {

        final Threshold[] thresholds = ThresholdChecker.getThresholds("thresholds.properties");

        final NumonicReporter reporter = new NumonicReporter("numonic.properties", thresholds);

        reporter.addObserver(this);

        reporter.start();
    }

    @Override
    public void update(final Observable o, final Object arg) {

        System.out.println("update: " + arg);
    }
}
