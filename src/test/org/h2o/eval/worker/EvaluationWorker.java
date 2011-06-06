package org.h2o.eval.worker;

import java.io.File;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;
import org.h2o.test.fixture.MultiProcessTestBase;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.JavaProcessDescriptor;

public class EvaluationWorker implements IWorker {

    private static final String PATH_TO_H2O_DATABASE = "evaluationWorker" + File.separator + "databaseFiles";

    private Process h2oProcess = null;

    /**
     * The name which will be given to the H2O instance being managed by this worker.
     */
    private final String h2oInstanceName;

    /**
     * JDBC connection string needed to communicate with the H2O database process.
     */
    private String connectionString;

    /**
     * JDBC connection to the running database process.
     */
    private Connection connection;

    /**
     * Count added to the end of the H2O instance created locally to make it possible to test this class with multiple instances running on the same machine.
     */
    private static int instanceCount = 0;

    public EvaluationWorker() {

        //Name the h2o instance after the machine it is being run on + a static counter number to enable same machine testing.
        h2oInstanceName = NetUtils.getLocalAddress().replaceAll("-", "").replaceAll("\\.", "") + instanceCount++;
    }

    @Override
    public void startH2OInstance(final String descriptorFileLocation) throws RemoteException, StartupException {

        if (h2oProcess != null) {
            //Check if its still running.
            throw new StartupException("Couldn't start H2O instance. Another instance is already running.");
        }

        final List<String> args = new LinkedList<String>();

        args.add("-i" + h2oInstanceName);
        args.add("-p" + PATH_TO_H2O_DATABASE);
        args.add("-d" + descriptorFileLocation);

        connectionString = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":9090:" + PATH_TO_H2O_DATABASE + "/" + h2oInstanceName;

        try {
            h2oProcess = new HostDescriptor().getProcessManager().runProcess(new JavaProcessDescriptor().classToBeInvoked(H2O.class).args(args));
        }
        catch (final Exception e) {
            throw new StartupException("Failed to create new H2O process: " + e.getMessage());
        }

        try {
            Thread.sleep(5000); // wait for the database to start up.
        }
        catch (final InterruptedException e) {
        }

        connection = MultiProcessTestBase.createConnectionToDatabase(connectionString);

        try {
            final Statement createStatement = connection.createStatement();
            createStatement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.USER");
        }
        catch (final SQLException e) {
            throw new StartupException("New H2O process couldn't be contacted once it had been created.");
        }

    }

    @Override
    public void stopH2OInstance() throws RemoteException, ShutdownException {

        if (h2oProcess == null) { throw new ShutdownException("Couldn't stop H2O process because the reference to it was null."); }

        // TODO Auto-generated method stub
    }

    @Override
    public void terminateH2OInstance() throws RemoteException, ShutdownException {

        if (h2oProcess == null) { throw new ShutdownException("Couldn't terminate H2O process because the reference to it was null."); }

        h2oProcess.destroy();
    }

    @Override
    public void deleteH2OInstanceState() throws RemoteException {

    }

    @Override
    public boolean isH2OInstanceRunning() throws RemoteException {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean startWorkload(final IWorkload workload) throws RemoteException {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean stopWorkload(final IWorkload workload) throws RemoteException {

        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isWorkloadRunning(final IWorkload workload) throws RemoteException {

        // TODO Auto-generated method stub
        return false;
    }

}
