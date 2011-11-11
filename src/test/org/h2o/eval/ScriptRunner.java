package org.h2o.eval;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

import org.h2.tools.DeleteDbFiles;
import org.h2o.eval.interfaces.IWorker;
import org.h2o.util.exceptions.StartupException;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

public class ScriptRunner {

    protected static int runCoordinationScript(final String databaseName, final List<InetAddress> workerLocationsInet, final Integer replicationFactor, final boolean startWorkersLocallyForTesting, final List<String> script, final String resultsFolderLocation,
                    final String coordinationScriptLocation, final Integer timeSlicePeriod) throws RemoteException, AlreadyBoundException, UnknownHostException, SQLException, IOException, StartupException {

        /*
        * Start workers locally if specified.
        */
        if (startWorkersLocallyForTesting) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Starting a number of worker instances locally for testing purposes.");

            final IWorker[] workers = new IWorker[8];
            for (int i = 0; i < workers.length; i++) {
                workers[i] = new Worker();
            }
        }

        /*
        * Create new Coordinator and start the specified number of database instances.
        */
        final ICoordinatorScript coord = new Coordinator(databaseName, workerLocationsInet);

        DeleteDbFiles.execute(Worker.PATH_TO_H2O_DATABASE, null, true);

        coord.setupSystem(34000, replicationFactor, replicationFactor);

        /*
        * Execute co-ordinator script.
        */
        try {
            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Results will be saved to: " + resultsFolderLocation);

            coord.executeCoordinationScript(script, resultsFolderLocation, coordinationScriptLocation);

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Blocking until workloads complete.");

            coord.blockUntilWorkloadsComplete(timeSlicePeriod);

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Workloads have completed");
        }
        catch (final Exception e) {
            ErrorHandling.exceptionError(e, "Failed to execute co-ordinator script.");

            return 1;
        }

        return 0;
    }
}
