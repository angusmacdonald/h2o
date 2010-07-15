/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Constants;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.comms.remote.DatabaseInstanceWrapper;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.h2o.manager.MovedException;
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
	private long replicatorSleepTime;

	/**
	 * Repeatedly attempts to replicate the System Tables state on the database instance at the specified location. This is done multiple times
	 * because the lookup may not work initially if the chord ring has not already recovered from a failure.
	 * @param hostname		Hostname of the database which will recieve the replica.
	 * @param port			Port on which the database instance is listening.
	 * @param iSystemTable			Used to update the System Table, informing it of the new replica location.
	 */
	public SystemTableReplication(String hostname, int port, ISystemTableReference systemTableRef, IChordInterface chordInterface, int replicatorSleepTime){
		this.hostname = hostname;
		this.port = port;
		this.systemTableRef = systemTableRef;
		this.chordInterface = chordInterface;
		this.replicatorSleepTime = replicatorSleepTime;
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
				Thread.sleep(replicatorSleepTime);
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
