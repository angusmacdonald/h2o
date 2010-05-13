package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Constants;
import org.h2.h2o.autonomic.Settings;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.manager.ISystemTable;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.SystemTableReference;
import org.h2.h2o.remote.IChordInterface;
import org.h2.test.h2o.ChordTests;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableReplication extends Thread {

	String hostname;
	int port;

	ISystemTableReference systemTableRef;
	IChordInterface chordInterface;

	/**
	 * Repeatedly attempts to replicate the System Tables state on the database instance at the specified location. This is done multiple times
	 * because the lookup may not work initially if the chord ring has not already recovered from a failure.
	 * @param hostname		Hostname of the database which will recieve the replica.
	 * @param port			Port on which the database instance is listening.
	 * @param iSystemTable			Used to update the System Table, informing it of the new replica location.
	 */
	public SystemTableReplication(String hostname, int port, ISystemTableReference systemTableRef, IChordInterface chordInterface){
		this.hostname = hostname;
		this.port = port;
		this.systemTableRef = systemTableRef;
		this.chordInterface = chordInterface;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		super.run();

		DatabaseInstanceRemote instance = null;
		int attempts = 0;

		do {
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempting to find database instance at " + hostname + ":" + port + ". Attempt " + (attempts+1) + " of " + 10);
			try {
				Thread.sleep(Settings.REPLICATOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			try {
				instance = chordInterface.getDatabaseInstanceAt(hostname, port);
			} catch (Exception e) {
				//Ignore.
			}
			attempts++;
		} while (instance == null && attempts < 10);

		if (attempts >= 10 && instance == null){
			ErrorHandling.error("Failed to get a reference to the database located at : " + hostname + ":" + port + " after " + attempts + " attempts.");
		} else {

			if (Diagnostic.getLevel() == DiagnosticLevel.FULL){
				try { Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found reference to remote database (where SM state will be replicated): " + instance.getConnectionString()); } catch (RemoteException e1) {}
			}

			try {
				createSystemTableReplicas(new DatabaseInstanceWrapper(instance.getConnectionURL(), instance, true));
			} catch (RemoteException e) {
				e.printStackTrace();
			}

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Successfully added new System Table replicas at " + hostname + ":" + port);
		}

	}

	/**
	 * @param instance
	 */
	private void createSystemTableReplicas(DatabaseInstanceWrapper instance) {
		try {
			systemTableRef.getSystemTable().addStateReplicaLocation(instance);
		} catch (MovedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (Constants.IS_TEST){
			ChordTests.setReplicated(true);
		}
	}

}
