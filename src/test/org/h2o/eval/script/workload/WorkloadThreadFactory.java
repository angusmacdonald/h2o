package org.h2o.eval.script.workload;

import java.util.concurrent.ThreadFactory;

public class WorkloadThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(final Runnable r) {

        return new Thread(r);
    }

}
