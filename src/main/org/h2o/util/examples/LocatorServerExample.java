/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.examples;

import org.h2o.H2OLocator;

/**
 * Starts a new locator server instance and creates a corresponding H2O descriptor file for this database domain.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class LocatorServerExample {

    public static void main(final String[] args) {

        // The name of the database domain.
        final String databaseName = "MyFirstDatabase";

        // The port on which the locator server will run.
        final int tcpPort = 9998;

        // Whether a database descriptor file will be created.
        final boolean createDescriptor = true;

        // Where the locator file (and descriptor file) will be created.
        final String rootFolder = "db_data";

        final H2OLocator locator = new H2OLocator(databaseName, tcpPort, createDescriptor, rootFolder);

        locator.start();
    }
}
