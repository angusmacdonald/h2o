package org.h2o.eval.tests;

import static org.junit.Assert.assertFalse;

import org.h2.tools.DeleteDbFiles;
import org.h2o.H2OLocator;
import org.h2o.eval.Worker;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.script.workload.Workload;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.StartupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class WorkerTests {

    private IWorker worker = null;
    private IWorker worker2 = null;

    @Before
    public void setUp() throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);

        DeleteDbFiles.execute(Worker.PATH_TO_H2O_DATABASE, null, true);

    }

    @After
    public void tearDown() throws Exception {

        DeleteDbFiles.execute(Worker.PATH_TO_H2O_DATABASE, null, true);

        if (worker != null) {
            try {
                worker.terminateH2OInstance();
                worker.stopWorkloadChecker();
            }
            catch (final Exception e) { //Will happen if an H2O instance isn't running.
            }
        }

        if (worker2 != null) {
            try {
                worker2.terminateH2OInstance();
                worker2.stopWorkloadChecker();
            }
            catch (final Exception e) { //Will happen if an H2O instance isn't running.
            }
        }
    }

    /**
     * Test that the {@link Worker} class is able to start up and query an H2O instance.
     * @throws Exception Not expected.
     */
    @Test
    public void testStartupH2O() throws Exception {

        worker = new Worker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, Worker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile, false);

        worker.terminateH2OInstance();
    }

    /**
     * Test that multiple instances of the {@link Worker} class are able to start up and query an H2O instance.
     * @throws Exception Not expected.
     */
    @Test
    public void testStartupMultipleWorkers() throws Exception {

        worker = new Worker();
        worker2 = new Worker();

        worker.deleteH2OInstanceState();
        worker2.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, Worker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile, false);
        Thread.sleep(5000);
        worker2.startH2OInstance(descriptorFile, false);

        worker.terminateH2OInstance();
        worker2.terminateH2OInstance();
    }

    /**
     * Test that the worker prevents multiple instances of the H2O process from being started at once.
     * @throws Exception Expect a startup exception to be thrown attempting to start the second instance.
     */

    @Test(expected = StartupException.class)
    public void startInstanceTwice() throws Exception {

        worker = new Worker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, Worker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile, false);
        worker.startH2OInstance(descriptorFile, false);

        worker.terminateH2OInstance();
    }

    /**
     * Tests the ability to tell the database to shut down.
     * @throws Exception Not expected.
     */

    @Test
    public void testStop() throws Exception {

        worker = new Worker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, Worker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile, false);

        worker.stopH2OInstance();

        Thread.sleep(5000); //wait a while for the H2O instance to shutdown.

        assertFalse(worker.isH2OInstanceRunning());
    }

    /**
     * Test that the {@link Worker} class is able to start up and query an H2O instance.
     * @throws Exception Not expected.
     */
    @Test
    public void testRunWorkload() throws Exception {

        worker = new Worker();

        worker.deleteH2OInstanceState();

        final H2OLocator locator = new H2OLocator("evaluationDB", 34000, true, Worker.PATH_TO_H2O_DATABASE);

        final String databaseDescriptorLocation = locator.start();

        final H2OPropertiesWrapper descriptorFile = H2OPropertiesWrapper.getWrapper(databaseDescriptorLocation);
        descriptorFile.loadProperties();

        worker.startH2OInstance(descriptorFile, false);

        final Workload workload = new Workload("src/test/org/h2o/eval/workloads/test.workload", 0);

        worker.startWorkload(workload);

        worker.terminateH2OInstance();
    }
}
