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
package org.h2o.db.manager.monitorthreads;

import org.h2o.db.manager.InMemorySystemTable;

/**
 * Periodically checks that Table Managers are still running. Attempts to re-instantiate them if they aren't.
 */
public class TableManagerLivenessCheckerThread extends Thread {

	private InMemorySystemTable inMemorySystemTable;

	private boolean running = true;
	
	private final int defaultSleepTime;
	private int sleepTime;
	
	public TableManagerLivenessCheckerThread(InMemorySystemTable inMemorySystemTable, int sleepTime) {
		this.inMemorySystemTable = inMemorySystemTable;
		this.sleepTime = sleepTime;
		this.defaultSleepTime = sleepTime;
	}

	public void run(){
		
		while (isRunning()){
			/*
			 * Sleep.
			 */
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {}
			
			boolean updated = inMemorySystemTable.checkTableManagerAccessibility();
			
			if (!updated && sleepTime < Integer.MAX_VALUE){
				sleepTime += 1000;
			} else if (updated) {
				sleepTime = defaultSleepTime;
			}
		}
	}
	
	
	/**
	 * @return the running
	 */
	public synchronized boolean isRunning() {
		return running ;
	}


	/**
	 * @param running the running to set
	 */
	public synchronized void setRunning(boolean running) {
		this.running = running;
	}

}
