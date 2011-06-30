package org.h2o.eval.madface;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

import uk.ac.standrews.cs.nds.madface.HostDescriptor;
import uk.ac.standrews.cs.nds.madface.HostState;
import uk.ac.standrews.cs.nds.madface.MadfaceManagerFactory;
import uk.ac.standrews.cs.nds.madface.interfaces.IApplicationManager;
import uk.ac.standrews.cs.nds.madface.interfaces.IMadfaceManager;

public class EvaluationNetwork {

    private final IMadfaceManager madface_manager;

    public EvaluationNetwork(final SortedSet<HostDescriptor> host_descriptors, final IApplicationManager workerManager) throws Exception {

        madface_manager = MadfaceManagerFactory.makeMadfaceManager();

        madface_manager.setHostScanning(true); //XXX does this need to be enabled?

        madface_manager.configureApplication(workerManager);

        for (final HostDescriptor new_node_descriptor : host_descriptors) {

            madface_manager.add(new_node_descriptor);
        }

        madface_manager.killAll(true); //blocks until it thinks it's killed everything.

        madface_manager.deployAll();
        madface_manager.waitForAllToReachState(HostState.RUNNING);
        madface_manager.setHostScanning(false);
    }

    /**
     * 
     * @param args Hostnames to start the network on.
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        final SortedSet<HostDescriptor> node_descriptors = new ConcurrentSkipListSet<HostDescriptor>();

        for (final String hostname : args) {
            final HostDescriptor hostDescriptor = new HostDescriptor(hostname);
            node_descriptors.add(hostDescriptor);
        }

        final IApplicationManager workerManager = new WorkerManager();

        new EvaluationNetwork(node_descriptors, workerManager); //returns when remote hosts have started.
    }

}
