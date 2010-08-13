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
package org.h2o.util.examples;
import org.h2o.H2OLocator;

/**
 * Starts a new locator server instance and creates a corresponding H2O descriptor file for this database domain.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LocatorServerExample {


	public static void main(String[] args) {

		String databaseName = "MyFirstDatabase";//the name of the database domain.
		int tcpPort = 9998;						//the port on which the locator server will run.
		boolean createDescriptor = true;		//whether a database descriptor file will be created.
		String rootFolder = "db_data"; 			//where the locator file (and descriptor file) will be created.
		boolean startInSeperateThread = false; 	//whether the locator server should be started in a seperate thread (it blocks).

		H2OLocator locator = new H2OLocator(databaseName, tcpPort, createDescriptor, rootFolder);

		locator.start(startInSeperateThread);

	}

}
