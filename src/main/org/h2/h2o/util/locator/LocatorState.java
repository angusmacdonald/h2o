package org.h2.h2o.util.locator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import org.h2.h2o.util.locator.messages.LockRequestResponse;
import org.h2.h2o.util.locator.messages.ReplicaLocationsResponse;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Used to write to database locator file. This class uses readers-writers model.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorState {	
	private int activeReaders = 0;
	private boolean writerPresent = false;  


	private File locatorFile;
	private boolean locked = false;
	private String databaseWithLock = null;
	int updateCount = 1;

	public final static int LOCK_TIMEOUT = 3000;
	private long lockCreationTime = 0l;

	protected LocatorState(String location){
		locatorFile = new File(location);
		if (!locatorFile.exists()){
			try {
				locatorFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}



	}

	private boolean isLocked(){
		return locked;
	}

	/**
	 * Read the set of database locations from the file.
	 * @return Set of db locations which hold system table replicas.
	 */
	public ReplicaLocationsResponse readLocationsFromFile() {
		startRead();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Reader reading:");

		Set<String> locations = new HashSet<String>();

		try {
			BufferedReader input = new BufferedReader(new FileReader(locatorFile));

			try {
				String line = null; 

				while (( line = input.readLine()) != null){
					locations.add(line);
				}
			}
			finally {
				input.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}


		ReplicaLocationsResponse response = new ReplicaLocationsResponse(locations, updateCount);


		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finished reading.");
		stopRead();


		return response;
	}

	/**
	 * Write the given array of database locations to the locator file.
	 * @param databaseLocations	Locations to be written to the file, each on a new line.
	 */
	public boolean writeLocationsToFile(String[] databaseLocations) {
		startWrite();

		boolean successful = false;

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Writer writing.");

		try {
			Writer output = new BufferedWriter(new FileWriter(locatorFile));

			try {
				for (String location: databaseLocations){
					output.write(location+ "\n");
				}
				successful = true;
			}

			finally {
				output.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finished writing.");
		stopWrite();
		
		return successful;
	}

	/**
	 * Create a lock file. This is used as a mechanism for re-creating System Tables.
	 * @param requestingDatabase	The database which is requesting the lock.
	 * @return true if the lock was successfully taken out; otherwise false.
	 */
	public LockRequestResponse lock(String requestingDatabase){
		startWrite();

		boolean success = false;

		if (locked){
			//Check that the lock hasn't timed out.
			if ((lockCreationTime + LOCK_TIMEOUT) < System.currentTimeMillis()){
				ErrorHandling.errorNoEvent("Lock held by " + databaseWithLock + " has timed out.");
				locked = false;
				databaseWithLock = null;
				lockCreationTime = 0l;
			}
		}
		
		
		if (locked){
			ErrorHandling.errorNoEvent("Lock already held by " + databaseWithLock + ".");
			success = false;
		} else {
			locked = true;
			lockCreationTime = System.currentTimeMillis();
			success = true;
			databaseWithLock = requestingDatabase;
		}

		LockRequestResponse response = new LockRequestResponse(updateCount, success);

		stopWrite();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Database instance at '" + requestingDatabase + "' has attempted to lock locator (successful: "+ success + ").");

		return response;
	}

	/**
	 * Release a lock file. Indicates that a System Table has been created successfully.
	 * @param requestingDatabase	The database which is requesting the lock.
	 * @return 0 if the commit failed; 1 if it succeeded.
	 */
	public int releaseLockOnFile(String requestingDatabase){
		startWrite();

		int result = 0;

		if (!locked || !requestingDatabase.equals(databaseWithLock)){
			ErrorHandling.errorNoEvent("Tried to release lock, but lock wasn't held by this database.");
			result = 0;
		} else {
			updateCount++;
			locked = false;
			lockCreationTime = 0l;
			databaseWithLock = null;
			result = 1;
		}

		stopWrite();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Database instance at '" + requestingDatabase + "' has attempted to unlock locator (successful: "+ (result==1) + ").");

		return result;
	}

	/*
	 * #####################################
	 * 
	 * Reader-writer methods.
	 * 
	 * From: http://beg.projects.cis.ksu.edu/examples/small/readerswriters/
	 * 
	 * #####################################
	 */

	private boolean writeCondition() {
		return activeReaders == 0 && !writerPresent;
	}

	private boolean readCondition() {
		return !writerPresent;
	}

	private synchronized void startRead() {
		while (!readCondition())
			try { wait(); } catch (InterruptedException ex) {}
			++activeReaders;
	}

	private synchronized void stopRead()  { 
		--activeReaders;
		notifyAll();
	}

	private synchronized void startWrite() {
		while (!writeCondition()) 
			try { wait(); } catch (InterruptedException ex) {}
			writerPresent = true;
	}

	private synchronized void stopWrite() { 
		writerPresent = false;
		notifyAll();
	}

	/**
	 * Create a new locator file. This is used by various test classes to overwrite old locator files.
	 */
	public void createNewLocatorFile() {
		startWrite();

		locatorFile.delete();
		try {
			locatorFile.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		stopWrite();
	}

}
