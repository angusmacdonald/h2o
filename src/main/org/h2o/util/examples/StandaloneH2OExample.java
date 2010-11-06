/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.util.examples;

import java.io.IOException;
import java.sql.SQLException;

import org.h2o.H2O;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Creates a standalone H2O instance.
 * 
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class StandaloneH2OExample {

    public static void main(final String[] args) throws SQLException, IOException {

        Diagnostic.setLevel(DiagnosticLevel.FINAL);

        // The name of the database domain.
        final String databaseName = "MyFirstDatabase";

        // The port on which the database's TCP JDBC server will run.
        final int tcpPort = 9999;

        // The port on which the database's web interface will run.
        final int webPort = 8282;

        // Where configuration files are stored.
        final String configurationDirectoryPath = "db_config";

        // Where the database will be created (where persisted state is stored).
        final String databaseDirectoryPath = "db_data";

        final H2O db = new H2O(databaseName, tcpPort, webPort, configurationDirectoryPath, databaseDirectoryPath);
        db.startDatabase();
    }
}
