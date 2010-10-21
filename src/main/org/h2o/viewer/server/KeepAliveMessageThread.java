package org.h2o.viewer.server;

import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

public class KeepAliveMessageThread extends Thread {

    private final H2OEvent keepAliveEvent;

    public KeepAliveMessageThread(final String dbLocation) {

        keepAliveEvent = new H2OEvent(dbLocation, DatabaseStates.KEEP_ALIVE, dbLocation);
    }

    @Override
    public void run() {

        while (true) {
            H2OEventBus.publish(keepAliveEvent);

            try {
                Thread.sleep(H2OEvent.KEEP_ALIVE_SLEEP_TIME);
            }
            catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
