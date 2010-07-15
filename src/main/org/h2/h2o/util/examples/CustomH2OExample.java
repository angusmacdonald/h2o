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
package org.h2.h2o.util.examples;

import java.io.File;

import org.h2.h2o.deployment.H2O;

/**
 * Creates a custom H2O instance. This requires a locator server to be run first. To do this run the {@link LocatorServerExample} class first.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class CustomH2OExample {

	public static void main(String[] args) {

		String databaseName = "MyFirstDatabase";		//the name of the database domain.
		int tcpPort = 9998;								//the port on which the database's TCP JDBC server will run.
		String rootFolder = "db_data"; 					//where the database will be created (where persisted state is stored).
		String descriptorLocation = "db_data" + 
		File.separator + "MyFirstDatabase.h2od"; 		//location of the database descriptor file.

		H2O db = new H2O(databaseName, tcpPort, 0, rootFolder, descriptorLocation);

		db.startDatabase();
	}

}
