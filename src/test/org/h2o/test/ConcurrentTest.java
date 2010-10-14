/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import java.sql.Statement;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ConcurrentTest extends Thread {
	
	private Statement stat;
	
	private int startNum;
	
	private int totalIterations;
	
	public boolean successful = true;
	
	private boolean update;
	
	public ConcurrentTest(Statement stat, int startNum, int totalIterations, boolean update) {
		this.stat = stat;
		this.startNum = startNum;
		this.totalIterations = totalIterations;
		this.update = update;
	}
	
	public void run() {
		for ( int i = startNum; i < ( totalIterations + startNum ); i++ ) {
			try {
				if ( update ) {
					successful = stat.execute("INSERT INTO TEST VALUES(" + i + ", 'hello');");
					stat.clearBatch();
				} else {
					successful = stat.execute("SELECT * FROM TEST;");
				}
				
			} catch ( Exception e ) {
				e.printStackTrace();
				successful = false;
			}
		}
	}
}
