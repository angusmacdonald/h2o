/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.h2o.test.fixture.TestBase;
import org.h2o.test.fixture.TestBase2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Various tests for the NO REPLICATE function in H2O.
 */
public class NoReplicateTests extends TestBase {

    private Connection cc;
    private Statement sc;

    /**
     * @throws java.lang.Exception
     */
    @Override
    @Before
    public void setUp() throws Exception {

        Constants.IS_TEAR_DOWN = false;
        TestBase.deleteDatabaseData("db_data/unittests/", "schema_test");
        setUpDescriptorFiles();
        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        org.h2.Driver.load();

        ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

        sa = ca.createStatement();
        sa.execute("SET REPLICATE FALSE");

        cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        // cc = DriverManager.getConnection("jdbc:h2:mem:three", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

        sa = ca.createStatement();
        sb = cb.createStatement();
        // sc = cc.createStatement();
        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sb.execute(sql);
    }

    /**
     * @throws SQLException
     * @throws InterruptedException
     * @throws java.lang.Exception
     */
    @Override
    @After
    public void tearDown() throws SQLException, InterruptedException {

        try {
            if (sa != null) {
                sa.execute("DROP ALL OBJECTS");
                sa.execute("SHUTDOWN");
                if (!sa.isClosed()) {
                    sa.close();
                }

                if (!ca.isClosed()) {
                    ca.close();
                }
            }
        }
        catch (final Exception e) {
            System.err.println("Error tearing down database: " + e.getMessage());
        }

        try {
            if (sb != null) {
                sb.execute("SHUTDOWN");
                if (!sb.isClosed()) {
                    sb.close();
                }

                if (!cb.isClosed()) {
                    cb.close();
                }

            }
        }
        catch (final Exception e) {
            System.err.println("Error tearing down database: " + e.getMessage());
        }

        try {
            if (sc != null) {
                sc.execute("SHUTDOWN");
                if (!sc.isClosed()) {
                    sc.close();
                }

                if (!cc.isClosed()) {
                    cc.close();
                }

            }
        }
        catch (final Exception e) {
            System.err.println("Error tearing down database: " + e.getMessage());
        }

        closeDatabaseCompletely();

        ca = null;
        cb = null;
        sa = null;
        sb = null;

        TestBase2.shutdownRegistry();

        ls.deletePersistedState();
        ls.setRunning(false);
        while (!ls.isFinished()) {
            Thread.sleep(SHUTDOWN_CHECK_DELAY);
        };
    }

    /**
     * Tests that the truncate table command works when a table has multiple replicas.
     */
    @Test
    public void noReplicateTest() {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        try {
            sa.executeQuery("SELECT LOCAL ONLY * FROM TEST;");

            fail("Nothing should be on the local machine.");
        }
        catch (final SQLException e1) {
            //Expected.
        }

        try {

            final String sql = "SET AUTOCOMMIT FALSE";
            sa.execute(sql);

            final ResultSet rs = sa.executeQuery("SELECT * FROM TEST;"); // Now query on first machine (which should have one extra row).

            if (!rs.next()) {
                fail("There should be some entries in this table.");
            }

            final String sql2 = "INSERT INTO TEST VALUES(3, 'Test!');";

            sa.execute(sql2);

            final ResultSet rs2 = sa.executeQuery("SELECT * FROM TEST;");

            if (!rs2.next()) {
                fail("There should be some entries in this table.");
            }

            cb.close();

            final ResultSet rs3 = sa.executeQuery("SELECT * FROM TEST;");

            if (!rs3.next()) {
                fail("There should be some entries in this table.");
            }

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");
        }
    }

}
