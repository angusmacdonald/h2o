package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Database;
import org.h2.h2o.remote.ChordInterface;

import uk.ac.standrews.cs.nds.p2p.exceptions.P2PNodeException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordNode;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaManagerReinstantiator extends Thread {

	Database db;
	ChordInterface chordInterface;

	public SchemaManagerReinstantiator(Database db, ChordInterface chordInterface){
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

			try {
				
//				//Check if schema manager is accessible.
//				try {
//					//This seems to show that there is a reference to a current machine which works, but which doesn't actually contain a running
//					//schema manager.
//					chordInterface.getCurrentSMLocation().getRemote().isAlive();
//					System.err.println("Schema manager machine: " + chordInterface.getCurrentSMLocation().getKey());
//				} catch (RemoteException e) {
//					e.printStackTrace();
//				}
				
				try {
					db.getSchemaManagerReference().getSchemaManager().exists(null);
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager is still accessible.");
					return;
				} catch (RemoteException e) {
					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Schema manager isn't accessible. It probably has to be reinstantiated, but by this node?");
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (chordInterface.getChordNode().inLocalKeyRange(ChordInterface.getSchemaManagerKey())){
					System.err.println("This is now the schema manager.");
				} else {
					System.err.println("Not in this keyspace");
					System.err.println("Predecessor: " + chordInterface.getChordNode().getPredecessor());
					System.err.println("Schema Manager Key: " + ChordInterface.getSchemaManagerKey());
					System.err.println("This node: " + chordInterface.getChordNode());
//					for (IChordNode n: ChordInterface.allNodes){
//						System.err.println((n));
//					}
				}
			} catch (P2PNodeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Re-instantiated Schema Manager");
		}

	}


}
