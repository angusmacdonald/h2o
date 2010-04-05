package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Constants;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.ISchemaManager;
import org.h2.h2o.remote.IChordInterface;
import org.h2.test.h2o.ChordTests;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerReplication extends Thread {

	String hostname;
	int port;

	ISchemaManager iSchemaManager;
	IChordInterface chordInterface;

	/**
	 * Repeatedly attempts to replicate the schema managers state on the database instance at the specified location. This is done multiple times
	 * because the lookup may not work initially if the chord ring has not already recovered from a failure.
	 * @param hostname		Hostname of the database which will recieve the replica.
	 * @param port			Port on which the database instance is listening.
	 * @param iSchemaManager			Used to update the schema manager, informing it of the new replica location.
	 */
	public SchemaManagerReplication(String hostname, int port, ISchemaManager iSchemaManager, IChordInterface chordInterface){
		this.hostname = hostname;
		this.port = port;
		this.iSchemaManager = iSchemaManager;
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
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempting to find database instance at " + hostname + ":" + port);
			try {
				Thread.sleep(1000);
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

			createSchemaManagerReplicas(instance);

			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Successfully added new schema manager replicas at " + hostname + ":" + port);
		}

	}

	/**
	 * @param instance
	 */
	private void createSchemaManagerReplicas(DatabaseInstanceRemote instance) {
		try {
			iSchemaManager.addSchemaManagerDataLocation(instance);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (Constants.IS_TEST){
			ChordTests.setReplicated(true);
		}
	}

}
