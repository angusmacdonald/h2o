package org.h2o.eval.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.db.id.DatabaseURL;
import org.h2o.eval.interfaces.ICoordinatorRemote;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.eval.interfaces.IWorkload;
import org.h2o.test.fixture.MultiProcessTestBase;
import org.h2o.util.H2OPropertiesWrapper;
import org.h2o.util.exceptions.ShutdownException;
import org.h2o.util.exceptions.StartupException;
import org.h2o.util.exceptions.WorkloadParseException;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.JavaProcessDescriptor;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class EvaluationWorker implements IWorker {

    public static final String PATH_TO_H2O_DATABASE = "eval";

    public static final String REGISTRY_PREFIX = "H2O_WORKER::";

    /**
     * Handle on the H2O process this worker is running.
     */
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

    private ICoordinatorRemote remoteCoordinator;

    public EvaluationWorker() throws RemoteException, AlreadyBoundException {

        //Name the h2o instance after the machine it is being run on + a static counter number to enable same machine testing.
        h2oInstanceName = NetUtils.getLocalAddress().replaceAll("-", "").replaceAll("\\.", "") + instanceCount++;

        Registry registry = null;

        try {
            registry = LocateRegistry.createRegistry(1099);
        }
        catch (final Exception e) {
            registry = LocateRegistry.getRegistry();
        }

        registry.bind(REGISTRY_PREFIX + h2oInstanceName, UnicastRemoteObject.exportObject(this, 0));

    }

    @Override
    public void initiateConnection(final ICoordinatorRemote remoteCoordinator) throws RemoteException {

        this.remoteCoordinator = remoteCoordinator;

    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (connectionString == null ? 0 : connectionString.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final EvaluationWorker other = (EvaluationWorker) obj;
        if (connectionString == null) {
            if (other.connectionString != null) { return false; }
        }
        else if (!connectionString.equals(other.connectionString)) { return false; }
        return true;
    }

    @Override
    public void startH2OInstance(final H2OPropertiesWrapper descriptor) throws RemoteException, StartupException {

        if (h2oProcess != null) {
            //Check if its still running.
            throw new StartupException("Couldn't start H2O instance. Another instance is already running.");
        }

        final String descriptorFileLocation = PATH_TO_H2O_DATABASE + File.separator + "descriptor.h2od";
        saveDescriptorToDisk(descriptor, descriptorFileLocation);

        h2oProcess = startH2OProcess(setupBootstrapArgs(descriptorFileLocation));

        sleep(5000);// wait for the database to start up.

        createConnectionString();

        connection = MultiProcessTestBase.createConnectionToDatabase(connectionString);

        final boolean isRunning = checkDatabaseIsActive();

        if (!isRunning) { throw new StartupException("New H2O process couldn't be contacted once it had been created."); }

    }

    /**
     * Create the JDBC connection string needed to connect to the H2O instance that has been started locally.
     */
    public void createConnectionString() {

        final int jdbc_port = H2O.getDatabasesJDBCPort(PATH_TO_H2O_DATABASE + "/" + DatabaseURL.getPropertiesFileName(PATH_TO_H2O_DATABASE) + "_" + h2oInstanceName + ".properties");
        connectionString = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + jdbc_port + "/" + PATH_TO_H2O_DATABASE + "/" + h2oInstanceName;
    }

    @Override
    public void stopH2OInstance() throws RemoteException, ShutdownException {

        if (h2oProcess == null) { throw new ShutdownException("Couldn't stop H2O process because the reference to it was null."); }

        try {
            final Statement stat = connection.createStatement();
            stat.executeUpdate("SHUTDOWN IMMEDIATELY;");
        }
        catch (final SQLException e) {
            throw new ShutdownException("The SHUTDOWN query attempting to shutdown the database instance failed: " + e.getMessage());
        }
    }

    @Override
    public void terminateH2OInstance() throws RemoteException, ShutdownException {

        if (h2oProcess == null) { throw new ShutdownException("Couldn't terminate H2O process because the reference to it was null."); }

        h2oProcess.destroy();
    }

    @Override
    public void deleteH2OInstanceState() throws RemoteException, ShutdownException {

        try {
            DeleteDbFiles.execute(EvaluationWorker.PATH_TO_H2O_DATABASE, null, true);
        }
        catch (final SQLException e) {
            throw new ShutdownException("Failed to delete database files: " + e.getMessage());
        }

    }

    @Override
    public boolean isH2OInstanceRunning() throws RemoteException {

        return checkDatabaseIsActive();
    }

    @Override
    public boolean startWorkload(final IWorkload workload) throws RemoteException, WorkloadParseException, SQLException {

        workload.execute(connection);
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

    /*
     * H2O Utility Methods.
     */

    /**
     * Start a new H2O instance locally, with the provided command line arguments.
     * @param args Arguments to be passed to {@link H2O} when it is started.
     * @return The process handle of the newly started process.
     * @throws StartupException If the process could not be started.
     */
    public Process startH2OProcess(final List<String> args) throws StartupException {

        try {
            return new HostDescriptor().getProcessManager().runProcess(new JavaProcessDescriptor().classToBeInvoked(H2O.class).args(args));
        }
        catch (final Exception e) {
            throw new StartupException("Failed to create new H2O process: " + e.getMessage());
        }
    }

    /**
     * Connects to the database instance using the {@link #connection} instance, and issues a read query. If the read
     * query fails a startup exception is thrown because the database is not active.
     * @throws StartupException If the database does not respond correctly to the query.
     */
    public boolean checkDatabaseIsActive() {

        try {

            if (connection == null) { return false; }

            final Statement createStatement = connection.createStatement();
            createStatement.executeQuery("SELECT * FROM INFORMATION_SCHEMA.USERS");
            return true;
        }
        catch (final SQLException e) {
            return false;
        }
    }

    /**
     * Sleep for the specified amount of time, ignoring InterruptedExceptions.
     * @param sleepTime in milliseconds.
     */
    public void sleep(final int sleepTime) {

        try {
            Thread.sleep(sleepTime);
        }
        catch (final InterruptedException e) {
        }
    }

    /**
     * Create a list of arguments needed to start {@link H2O}.
     * @param descriptorFileLocation The location of the descriptor file for this database system.
     * @return The completed list of command line arguments.
     */
    public List<String> setupBootstrapArgs(final String descriptorFileLocation) {

        final List<String> args = new LinkedList<String>();
        args.add("-i" + h2oInstanceName);
        args.add("-f" + PATH_TO_H2O_DATABASE);

        args.add("-d" + descriptorFileLocation);

        return args;
    }

    /**
     * Save the descriptor file to disk at the specified location.
     * @param descriptor The properties file to be saved to disk.
     * @param descriptorFileLocation Where the properties file should be saved.
     */
    public void saveDescriptorToDisk(final H2OPropertiesWrapper descriptor, final String descriptorFileLocation) {

        try {
            final FileOutputStream descriptorFile = new FileOutputStream(descriptorFileLocation);
            descriptor.saveAndClose(descriptorFile);
        }
        catch (final IOException e1) {
            ErrorHandling.exceptionErrorNoEvent(e1, "Couldn't save descriptor file locally.");
        }
    }

}
