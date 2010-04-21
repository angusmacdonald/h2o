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

/**
 * Used to write to database locator file. This class uses readers-writers model.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class LocatorFileWriter {

	private File locatorFile;
	private int activeReaders = 0;  
	
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
	}

	/**
	 * Read the set of database locations from the file.
	 * @return Set of db locations which hold system table replicas.
	 */
	public Set<String> readFromFile() {
		startRead();
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Reader reading:");

		Set<String> locations = new HashSet<String>();

		try {
			BufferedReader input;

			input = new BufferedReader(new FileReader(locatorFile));

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
	public void writeToFile(String[] databaseLocations) {
		startWrite();
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Writer writing.");

		try {
			//use buffering
			Writer output;

			output = new BufferedWriter(new FileWriter(locatorFile));

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
