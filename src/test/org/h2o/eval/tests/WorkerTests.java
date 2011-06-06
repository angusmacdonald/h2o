package org.h2o.eval.tests;

import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.worker.EvaluationWorker;
import org.junit.Before;
import org.junit.Test;

public class WorkerTests {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testy() throws Exception {

        final IWorker worker = new EvaluationWorker();

        worker.deleteH2OInstanceState();

        worker.startH2OInstance("evaluationDescriptor.h2od");

        worker.terminateH2OInstance();
    }
}
