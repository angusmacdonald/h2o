package org.h2o.monitoring;

import java.io.IOException;
import java.util.Observable;
import java.util.Observer;

import org.h2o.autonomic.numonic.NumonicReporter;
import org.h2o.autonomic.numonic.ThresholdChecker;
import org.h2o.autonomic.numonic.threshold.Threshold;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class ExternalNumonicTest implements Observer {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(final String[] args) throws IOException {

        Diagnostic.setLevel(DiagnosticLevel.FULL);
        final ExternalNumonicTest test = new ExternalNumonicTest();
        test.start();
    }

    public void start() throws IOException {

        final Threshold[] thresholds = ThresholdChecker.getThresholds("default_numonic_thresholds.properties");

        final NumonicReporter reporter = new NumonicReporter("default_numonic_settings.properties", "C:\\", null, null, thresholds);

        reporter.addObserver(this);

        reporter.start();
    }

    @Override
    public void update(final Observable o, final Object arg) {

        System.out.println("update: " + arg);
    }
}
