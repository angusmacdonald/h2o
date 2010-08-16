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
package org.h2o.event.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import org.h2o.event.client.H2OEvent;
import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Used to write to database locator file. This class uses readers-writers
 * model.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class EventViewer {
	private int activeReaders = 0;
	private boolean writerPresent = false;

	private File file;
	int updateCount = 1;

	public final static int LOCK_TIMEOUT = 3000;

	protected EventViewer(String location) {
		file = new File(location);

		if (file.getParentFile() != null) {
			file.getParentFile().mkdirs();
		}

		try {
			if (!(file.createNewFile() || file.isFile())) {
				ErrorHandling
						.errorNoEvent("This is a directory, when I file should have been given.");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	public List<String> readEventsFromFile() {
		startRead();

		// Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Reader reading:");

		List<String> events = new LinkedList<String>();

		try {
			BufferedReader input = new BufferedReader(new FileReader(file));

			try {
				String line = null;

				while ((line = input.readLine()) != null) {
					events.add(line);
				}
			} finally {
				input.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		stopRead();

		return events;
	}

	public boolean pushEvent(H2OEvent event) {

		boolean successful = false;

		try {
			Writer output = new BufferedWriter(new FileWriter(file));

			try {
				output.write(event.toString() + "\n");
				successful = true;
			}

			finally {
				output.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		stopWrite();

		return successful;
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
			try {
				wait();
			} catch (InterruptedException ex) {
			}
		++activeReaders;
	}

	private synchronized void stopRead() {
		--activeReaders;
		notifyAll();
	}

	private synchronized void startWrite() {
		while (!writeCondition())
			try {
				wait();
			} catch (InterruptedException ex) {
			}
		writerPresent = true;
	}

	private synchronized void stopWrite() {
		writerPresent = false;
		notifyAll();
	}

	/**
	 * Create a new locator file. This is used by various test classes to
	 * overwrite old locator files.
	 */
	public void createNewLocatorFile() {
		startWrite();

		file.delete();
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}

		stopWrite();
	}

	@Override
	public String toString() {
		return file.getAbsolutePath();
	}

}
