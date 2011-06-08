package org.h2o.eval.tests;

import org.h2.tools.DeleteDbFiles;
import org.h2o.H2OLocator;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.worker.EvaluationWorker;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WorkerTests {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

        DeleteDbFiles.execute(EvaluationWorker.PATH_TO_H2O_DATABASE, null, true);

    }

    /**
     * Test that the {@link EvaluationWorker} class is able to start up and query an H2O instance.
     * @throws Exception Not expected.
     */
    @Test
    public void testStartupH2O() throws Exception {

        final IWorker worker = new EvaluationWorker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, EvaluationWorker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile);

        worker.terminateH2OInstance();
    }

    /**
     * Test that multiple instances of the {@link EvaluationWorker} class are able to start up and query an H2O instance.
     * @throws Exception Not expected.
     */
    @Test
    public void testStartupMultipleWorkers() throws Exception {

        final IWorker worker1 = new EvaluationWorker();
        final IWorker worker2 = new EvaluationWorker();

        worker1.deleteH2OInstanceState();
        worker2.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, EvaluationWorker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker1.startH2OInstance(descriptorFile);
        worker2.startH2OInstance(descriptorFile);

        worker1.terminateH2OInstance();
        worker2.terminateH2OInstance();
    }

    /**
     * Test that the worker prevents multiple instances of the H2O process from being started at once.
     * @throws Exception Expect a startup exception to be thrown attempting to start the second instance.
     */
    @Test(expected = StartupException.class)
    public void startInstanceTwice() throws Exception {

        final IWorker worker = new EvaluationWorker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, EvaluationWorker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile);
        worker.startH2OInstance(descriptorFile);

        worker.terminateH2OInstance();
    }
}
