package org.h2.h2o.util;

import java.rmi.RemoteException;

import org.h2.engine.Constants;
import org.h2.h2o.comms.remote.DatabaseInstanceRemote;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.remote.IChordInterface;
import org.h2.h2o.remote.IDatabaseRemote;
import org.h2.test.h2o.ChordTests;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;
import uk.ac.standrews.cs.stachordRMI.interfaces.IChordRemoteReference;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LookupPinger extends Thread {

	/**
	 * Time between pings.
	 */
	private static final long DEFAULT_SLEEP_TIME = 5000;

	private static final int MAXIMUM_CONSECUTIVE_ERRORS = 10;
	
	/**
	 * Interface to the rest of the database system for this instance.
	 */
	private IDatabaseRemote remoteInterface;
	
	/**
	 * Interface to the Chord system.
	 */
	private IChordInterface chordInterface;
	
	/**
	 * Location of the schema manager instance on the chord ring.
	 */
	private IChordRemoteReference schemaManagerLocation;
	
	/**
	 * Whether the pinger thread should still be running.
	 */
	private boolean running = true;

	/**
	 * @param remoteInterface
	 * @param location
	 */
	public LookupPinger(IDatabaseRemote remoteInterface, IChordInterface chordInterface,
			IChordRemoteReference location) {
		this.remoteInterface = remoteInterface;
		this.chordInterface = chordInterface;
		this.schemaManagerLocation = location;
		
		if (Constants.IS_TEST){
			ChordTests.pingSet.add(this);
		}
	}
	
	
	@Override
	public void run(){

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Starting to ping schema manager lookup location.");

		int errors = 0;
		
		while (isRunning()){
			/*
			 * Continously inform schema managers chord ring node of the 
			 * actual schema manager location.
			 */

			
			try {
				Thread.sleep(DEFAULT_SLEEP_TIME);
			} catch (InterruptedException e) {
				ErrorHandling.errorNoEvent("Pinger thread was interrupted while sleeping.");
			}
			
			IChordRemoteReference lookupLocation = null;
			try {
				lookupLocation = chordInterface.getLookupLocation(SchemaManagerReference.schemaManagerKey);
			} catch (RemoteException e1) {
				errors ++;
				ErrorHandling.errorNoEvent("Error on ping lookup.");
				continue;
			}
			
			if (lookupLocation == null){
				ErrorHandling.errorNoEvent("Lookup location was null.");
			} else if (!schemaManagerLocation.equals(lookupLocation)){
				//If this chord node doesn't hold both the lookup and the schema manager process.
				
				try {
//					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Lookup location is: " + lookupLocation.getRemote().getAddress().getPort());
//					Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "SM location is: " + schemaManagerLocation.getRemote().getAddress().getPort());
					DatabaseInstanceRemote dir = remoteInterface.getDatabaseInstanceAt(lookupLocation);
					dir.setSchemaManagerLocation(schemaManagerLocation, remoteInterface.getLocalMachineLocation());
					errors = 0;
				} catch (RemoteException e) {
					ErrorHandling.errorNoEvent("Pinger thread failed to find instance responsible for schema manager lookup.");
					errors ++;
				}
			} else {
				errors = 0;
			}
			
			if (errors > MAXIMUM_CONSECUTIVE_ERRORS){
				setRunning(false);
			}
		}
		
	}


	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running;
	}


	/**
	 * @param running the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}
}