/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test.fixture;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2o.autonomic.settings.TestingSettings;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.util.H2OPropertiesWrapper;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * When an instance of this class is created 9 H2O in-memory instances are also created. These can then be used in testing.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultipleServersStandalone {

    private Connection[] cas;

    private Statement[] sas;

    private final String[] dbs = {"two", "three", "four", "five", "six", "seven", "eight", "nine"};

    public MultipleServersStandalone() throws SQLException, IOException {

        initialSetUp();

        setUp();
    }

    public void initialSetUp() throws IOException {

        Diagnostic.setLevel(DiagnosticLevel.INIT);

        createMultiplePropertiesFiles(dbs);

    }

    private void createMultiplePropertiesFiles(final String[] dbNames) throws IOException {

        for (final String db : dbNames) {

            final String fullDBName = "jdbc:h2:mem:" + db;
            final DatabaseID dbURL = DatabaseID.parseURL(fullDBName);

            final H2OPropertiesWrapper knownHosts = H2OPropertiesWrapper.getWrapper(dbURL);
            knownHosts.createNewFile();
            knownHosts.setProperty("jdbc:h2:sm:mem:one", "30000"); // //jdbc:h2:sm:mem:one
            knownHosts.saveAndClose();
        }
    }

    public void setUp() throws SQLException {

        TestingSettings.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";

        org.h2.Driver.load();

        cas = new Connection[dbs.length + 1];
        cas[0] = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        for (int i = 1; i < cas.length; i++) {

            cas[i] = DriverManager.getConnection("jdbc:h2:mem:" + dbs[i - 1], PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        }

        sas = new Statement[dbs.length + 1];
    }

    public void tearDown() throws SQLException {

        for (int i = 0; i < sas.length; i++) {
            if (!sas[i].isClosed()) {
                sas[i].close();
            }
            sas[i] = null;
        }

        for (int i = 0; i < cas.length; i++) {

            if (!cas[i].isClosed()) {
                cas[i].close();
            }
            cas[i] = null;
        }

        cas = null;
        sas = null;
    }

    /**
     * @param args
     * @throws InterruptedException
     * @throws IOException 
     * @throws SQLException 
     */
    public static void main(final String[] args) throws InterruptedException, SQLException, IOException {

        Constants.IS_TEST = true;
        final MultipleServersStandalone servers = new MultipleServersStandalone();

        Thread.sleep(10000);

        servers.testSystemTableFailure();
    }

    /**
     * 
     */
    private void testSystemTableFailure() {

        Diagnostic.trace("CLOSING System Table INSTANCE");

        try {
            cas[0].close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
    }
}
