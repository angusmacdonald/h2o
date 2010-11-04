/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2o.locator.server.LocatorServer;
import org.h2o.test.util.StartDatabaseInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Class which conducts tests on 10 in-memory databases running at the same time.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordTestsFailing extends TestBase {

    private Statement[] sas;

    private StartDatabaseInstance[] dts;

    private LocatorServer ls;

    private static String[] dbs = {"two", "three"}; // , "four", "five", "six", "seven", "eight", "nine"

    @BeforeClass
    public static void initialSetUp() {

        Diagnostic.setLevel(DiagnosticLevel.INIT);
        Constants.IS_TEST = true;
        Constants.IS_NON_SM_TEST = false;
    }

    @Override
    @Before
    public void setUp() throws Exception {

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();

        Constants.IS_TEAR_DOWN = false;

        org.h2.Driver.load();

        TestBase.setUpDescriptorFiles();
        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        dts = new StartDatabaseInstance[dbs.length + 1];
        dts[0] = new StartDatabaseInstance("jdbc:h2:sm:mem:one", false);
        dts[0].start();

        Thread.sleep(5000);

        for (int i = 1; i < dts.length; i++) {

            dts[i] = new StartDatabaseInstance("jdbc:h2:mem:" + dbs[i - 1], false);
            dts[i].start();

            Thread.sleep(5000);
        }

        sas = new Statement[dbs.length + 1];

        for (int i = 0; i < dts.length; i++) {
            sas[i] = dts[i].getConnection().createStatement();
        }

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sas[0].execute(sql);
    }

    @Override
    @After
    public void tearDown() {

        Constants.IS_TEAR_DOWN = true;

        for (final StartDatabaseInstance dt : dts) {
            dt.setRunning(false);
        }

        closeDatabaseCompletely();

        ls.setRunning(false);
        dts = null;
        sas = null;

        ls.setRunning(false);
        while (!ls.isFinished()) {
        };
    }

    @Test
    public void tableManagerMigrationWithCachedReference() throws SQLException {

        sas[0].executeUpdate("INSERT INTO TEST VALUES(7, '7');");
        sas[1].executeUpdate("INSERT INTO TEST VALUES(6, '6');");
        sas[2].executeUpdate("INSERT INTO TEST VALUES(8, '8');");

        sas[1].executeUpdate("MIGRATE TABLEMANAGER test");

        /*
         * Test that the new Table Manager can be found.
         */
        sas[2].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");

        /*
         * Test that the old Table Manager is no longer accessible, and that the reference can be updated.
         */
        sas[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");

        final ResultSet rs = sas[0].executeQuery("SELECT manager_location FROM H2O.H2O_TABLE");

        assertTrue("System Table wasn't updated correctly.", rs.next());
        assertEquals(2, rs.getInt(1));
    }
}
