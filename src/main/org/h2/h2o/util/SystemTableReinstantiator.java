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


import org.h2.engine.Database;
import org.h2.h2o.manager.SystemTableReference;
import org.h2.h2o.remote.IChordInterface;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SystemTableReinstantiator extends Thread {

	Database db;
	IChordInterface chordInterface;

	public SystemTableReinstantiator(Database db, IChordInterface chordInterface){
		this.db = db;
		this.chordInterface = chordInterface;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		super.run();

		IChordRemoteReference instance = null;
		int attempts = 0;

		do {
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Attempting to find predecessor to local node.");
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			instance = chordInterface.getChordNode().getPredecessor();
			attempts++;
		} while (instance == null && attempts < 10);

		if (attempts >= 10 && instance == null){
			ErrorHandling.error("Failed to get a predecessor to: " + chordInterface.getChordNode());
		} else {


			//				//Check if System Table is accessible.
			//				try {
			//					//This seems to show that there is a reference to a current machine which works, but which doesn't actually contain a running
			//					//System Table.
			//					chordInterface.getCurrentSMLocation().getRemote().isAlive();
			//					System.err.println("System Table machine: " + chordInterface.getCurrentSMLocation().getKey());
			//				} catch (RemoteException e) {
			//					e.printStackTrace();
			//				}

			try {
				db.getSystemTableReference().getSystemTable().exists(null);

				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "System Table is still accessible.");
				return;
			} catch (Exception e) {
				Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "System Table isn't accessible. It probably has to be reinstantiated, but by this node?");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (chordInterface.getChordNode().inLocalKeyRange(SystemTableReference.systemTableKey)){
				System.err.println("This is now the System Table.");
			} else {
				System.err.println("Not in this keyspace");
				System.err.println("Predecessor: " + chordInterface.getChordNode().getPredecessor());
				System.err.println("System Table Key: " + SystemTableReference.systemTableKey);
				System.err.println("This node: " + chordInterface.getChordNode());
				//					for (IChordNode n: ChordInterface.allNodes){
				//						System.err.println((n));
				//					}
			}
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Re-instantiated System Table");
		}

	}


}
