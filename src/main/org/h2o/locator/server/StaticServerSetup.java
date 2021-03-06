/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.locator.server;

import java.io.File;
import java.io.IOException;

import org.h2o.db.id.DatabaseID;
import org.h2o.locator.DatabaseDescriptor;
import org.h2o.util.H2OPropertiesWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class StaticServerSetup {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        setUpStaticDescriptorFiles();
    }

    /**
     * @throws IOException
     * 
     */
    public static void setUpStaticDescriptorFiles() throws IOException {

        final String databaseName = "angusDB";

        final String descriptorFile = "http://www.cs.st-andrews.ac.uk/~angus/databases/" + databaseName + ".h2o";

        final String initialSchemaManager = "jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db";

        /*
         * Clear locator file.
         */

        final File f = new File("config\\locatorFile.locator");
        final boolean successful = f.delete();

        if (!successful) {
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to delete locator file.");
        }

        /*
         * Setup descriptor file.
         */
        final DatabaseDescriptor ddf = new DatabaseDescriptor("\\\\shell\\angus\\public_html\\databases\\" + databaseName + ".h2o");
        ddf.createPropertiesFile();
        ddf.setLocatorLocations(databaseName, "eigg:29999");

        /*
         * Setup bootstrap files.
         */
        H2OPropertiesWrapper knownHosts = H2OPropertiesWrapper.getWrapper(DatabaseID.parseURL(initialSchemaManager));
        knownHosts.createNewFile();
        knownHosts.setProperty("descriptor", descriptorFile);
        knownHosts.setProperty("databaseName", databaseName);
        knownHosts.saveAndClose();

        knownHosts = H2OPropertiesWrapper.getWrapper(DatabaseID.parseURL("jdbc:h2:tcp://localhost:9191/db_data/three/test_db"));
        knownHosts.createNewFile();
        knownHosts.setProperty("descriptor", descriptorFile);
        knownHosts.setProperty("databaseName", databaseName);
        knownHosts.saveAndClose();

        knownHosts = H2OPropertiesWrapper.getWrapper(DatabaseID.parseURL("jdbc:h2:tcp://localhost:9292/db_data/two/test_db"));
        knownHosts.createNewFile();
        knownHosts.setProperty("descriptor", descriptorFile);
        knownHosts.setProperty("databaseName", databaseName);
        knownHosts.saveAndClose();
    }
}
