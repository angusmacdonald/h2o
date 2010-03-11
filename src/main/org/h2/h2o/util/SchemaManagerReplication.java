package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.remote.ChordInterface;
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

	Database db;
	ChordInterface chordInterface;

	public SchemaManagerReplication(String hostname, int port, Database db, ChordInterface chordInterface){
		this.hostname = hostname;
		this.port = port;
		this.db = db;
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

			instance = chordInterface.getRemoteReferenceToDatabaseInstance(hostname, port);
			attempts++;
		} while (instance == null && attempts < 10);

		if (attempts >= 10 && instance == null){
			ErrorHandling.error("Failed to get a reference to the database located at : " + hostname + ":" + port + " after " + attempts + " attempts.");
		} else {

			if (Diagnostic.getLevel() == DiagnosticLevel.FULL){
				try {
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Found reference to remote database (where SM state will be replicated): " + instance.getConnectionString());
				} catch (RemoteException e1) {}
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
			this.db.getSchemaManager().addSchemaManagerDataLocation(instance);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		if (Constants.IS_TEST){
			ChordTests.setReplicated(true);
		}
	}

}
