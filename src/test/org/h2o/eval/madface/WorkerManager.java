package org.h2o.eval.madface;

import java.io.File;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import org.h2o.eval.Worker;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.JavaProcessDescriptor;
import uk.ac.standrews.cs.nds.madface.ProcessDescriptor;
import uk.ac.standrews.cs.nds.madface.interfaces.IApplicationManager;
import uk.ac.standrews.cs.nds.madface.interfaces.IGlobalHostScanner;
import uk.ac.standrews.cs.nds.madface.interfaces.ISingleHostScanner;

public class WorkerManager implements IApplicationManager {

    private static final String JAVA_EXECUTABLE_DIR = "/usr/java/latest/bin";

    /**
     * Path to directory containing Java executable.
     */
    public static final File JAVA_BIN_PATH = new File(JAVA_EXECUTABLE_DIR);

    @Override
    public String getApplicationName() {

        return "H2O-Eval-Worker";
    }

    @Override
    public void attemptApplicationCall(final HostDescriptor host_descriptor) throws Exception {

        final Registry r = LocateRegistry.getRegistry(host_descriptor.getHost());

        r.list(); // will fail if there isn't a registry running.
    }

    @Override
    public void deployApplication(final HostDescriptor host_descriptor) throws Exception {

        host_descriptor.javaBinPath(JAVA_BIN_PATH);
        final ProcessDescriptor java_process_descriptor = new JavaProcessDescriptor().classToBeInvoked(Worker.class).args(new ArrayList<String>());
        host_descriptor.getProcessManager().runProcess(java_process_descriptor);

    }

    @Override
    public void killApplication(final HostDescriptor host_descriptor, final boolean kill_all_instances) throws Exception {

        host_descriptor.killProcesses(Worker.class.getCanonicalName());

    }

    @Override
    public List<ISingleHostScanner> getSingleScanners() {

        return null;
    }

    @Override
    public List<IGlobalHostScanner> getGlobalScanners() {

        return null;
    }

    @Override
    public void shutdown() {

        // Nothing to shut down, I think.

    }

}
