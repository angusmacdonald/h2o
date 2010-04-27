package org.h2.h2o.util.properties.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Used to write to database locator file. This class uses readers-writers model.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorFileWriter {

	private File locatorFile;
	private File lockFile;
	private int activeReaders = 0;
	
	private final int LOCK_FILE_TIMEOUT = 10000;

	private long lockFileCreationTime = 0l;
	
	private boolean writerPresent = false;  

	protected LocatorFileWriter(String location){
		locatorFile = new File(location);
		if (!locatorFile.exists()){
			try {
				locatorFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		lockFile = new File(location + ".lock");
		if (lockFile.exists()){
			lockFile.delete();
		}

	}

	private boolean isLocked(){
		return lockFile.exists();
	}
	
	/**
	 * Read the set of database locations from the file.
	 * @return Set of db locations which hold system table replicas.
	 */
	public Set<String> readLocationsFromFile() {
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

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finished reading.");
		stopRead();


		return locations;
	}

	/**
	 * Write the given array of database locations to the locator file.
	 * @param databaseLocations	Locations to be written to the file, each on a new line.
	 */
	public void writeLocationsToFile(String[] databaseLocations) {
		startWrite();
		
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Writer writing.");

		try {
			Writer output = new BufferedWriter(new FileWriter(locatorFile));

			try {
				for (String location: databaseLocations){
					output.write(location+ "\n");
				}
			}
			finally {
				output.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Finished writing.");
		stopWrite();
	}

	/**
	 * Create a lock file. This is used as a mechanism for re-creating System Tables.
	 * @param requestingDatabase	The database which is requesting the lock.
	 * @return true if the lock was successfully taken out; otherwise false.
	 */
	public boolean takeOutLockOnFile(String requestingDatabase){
		startWrite();

		boolean success = false;

		if (lockFile.exists()){
			success = false;
		} else {

			try {
				lockFile.createNewFile();

				Writer output = new BufferedWriter(new FileWriter(lockFile));

				try {
					output.write(requestingDatabase);
				} finally {
					output.close();
				}

				success = true;
			} catch (IOException e) {
				e.printStackTrace();
				ErrorHandling.exceptionErrorNoEvent(e, "Failed to create lock file.");
				success= false;
			}

			lockFile.deleteOnExit();
		}

		
		stopWrite();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Database instance at '" + requestingDatabase + "' has attempted to lock locator (successful: "+ success + ").");

		return success;
	}

	/**
	 * Release a lock file. Indicates that a System Table has been created successfully.
	 * @param requestingDatabase	The database which is requesting the lock.
	 * @return true if the lock was successfully released; otherwise false.
	 */
	public boolean releaseLockOnFile(String requestingDatabase){
		startWrite();

		boolean success = false;

		if (!lockFile.exists()){
			ErrorHandling.errorNoEvent("Tried to release lock, but lock file didn't exist.");
			success = false;
		} else {

			try {
				BufferedReader input = new BufferedReader(new FileReader(lockFile));
				String line = null;

				try {
					line = input.readLine();


				}
				finally {
					input.close();
				}

				if (requestingDatabase.equals(line)){
					success = lockFile.delete();
				} else {
					ErrorHandling.errorNoEvent("The database requesting that the lock is removed is different from the database which took out the lock (" +
							"Requesting DB: " + requestingDatabase + ", DB which took out lock: " + line + ").");
				}

			} catch (IOException e) {
				e.printStackTrace();
				ErrorHandling.exceptionErrorNoEvent(e, "Failed to remove lock file.");
				success = false;
			}
		}

		stopWrite();

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Database instance at '" + requestingDatabase + "' has attempted to unlock locator (successful: "+ success + ").");

		return success;
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
