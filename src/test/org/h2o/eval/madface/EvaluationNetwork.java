package org.h2o.eval.madface;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.HostState;
import uk.ac.standrews.cs.nds.madface.MadfaceManagerFactory;
import uk.ac.standrews.cs.nds.madface.interfaces.IApplicationManager;
import uk.ac.standrews.cs.nds.madface.interfaces.IMadfaceManager;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class EvaluationNetwork {

    private final IMadfaceManager madface_manager;

    public EvaluationNetwork(final SortedSet<HostDescriptor> host_descriptors, final IApplicationManager workerManager) throws Exception {

        madface_manager = MadfaceManagerFactory.makeMadfaceManager();

        madface_manager.setHostScanning(true); //XXX does this need to be enabled?

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Set host scanning to true.");

        madface_manager.configureApplication(workerManager);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Configured application.");

        for (final HostDescriptor new_node_descriptor : host_descriptors) {

            Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Added host descriptor: " + new_node_descriptor.getHost());

            madface_manager.add(new_node_descriptor);
        }

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Sent kill command to all nodes.");

        madface_manager.killAll(true); //blocks until it thinks it's killed everything.

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Kill command executed on all nodes");

        madface_manager.deployAll();

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "System deployed successfully on all nodes.");

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Waiting for all nodes to start up.");

        madface_manager.waitForAllToReachState(HostState.RUNNING);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "All nodes have started.");

        madface_manager.setHostScanning(false);

        Diagnostic.traceNoEvent(DiagnosticLevel.FINAL, "Host scanning disabled, Evaluation Network complete.");

    }

    /**
     * 
     * @param args Hostnames to start the network on.
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        Diagnostic.setLevel(DiagnosticLevel.FULL);

        final SortedSet<HostDescriptor> node_descriptors = new ConcurrentSkipListSet<HostDescriptor>();

        for (final String hostname : args) {
            final HostDescriptor hostDescriptor = new HostDescriptor(hostname);
            node_descriptors.add(hostDescriptor);
        }

        final IApplicationManager workerManager = new WorkerManager();

        new EvaluationNetwork(node_descriptors, workerManager); //returns when remote hosts have started.
    }

}
