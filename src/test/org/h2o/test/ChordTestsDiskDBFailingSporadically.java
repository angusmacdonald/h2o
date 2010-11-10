/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

/**
 * Tests on multiple databases.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordTestsDiskDBFailingSporadically extends H2OTestBase {

    private TestDriver[] drivers;
    private Statement[] statements;

    private static final int number_of_databases = 3;

    @Override
    protected int getNumberOfDatabases() {

        return number_of_databases;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException {

        super.setUp();

        drivers = new TestDriver[number_of_databases];
        statements = new Statement[number_of_databases];

        for (int i = 0; i < number_of_databases; i++) {
            drivers[i] = makeTestDriver(i);
            statements[i] = drivers[i].getConnection().createStatement();
        }

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        statements[0].execute(sql);

        Constants.IS_TEST = true;
        Constants.IS_NON_SM_TEST = false;
        Constants.IS_TEAR_DOWN = false;
    }

    @Override
    @After
    public void tearDown() throws SQLException {

        Constants.IS_TEAR_DOWN = true;

        super.tearDown();
    }

    @Test(timeout = 60000)
    public void baseTest() throws SQLException {

        Diagnostic.trace();

        statements[0].execute("SELECT * FROM TEST;");
    }

    /**
     * This sequence of events used to lock the sys table causing entries to not be included in the System Table. If this fails or holds
     * then the problem is still there.
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void sysTableLock() throws SQLException {

        Diagnostic.trace();

        Constants.IS_NON_SM_TEST = true;

        statements[1].execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        statements[2].execute("CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255));");

        final ResultSet rs = statements[0].executeQuery("SELECT * FROM H2O.H2O_TABLE");

        assertTrue(rs.next() && rs.next() && rs.next());
        assertFalse(rs.next());
    }

    /**
     * Tests that when the Table Manager is migrated another database instance is able to connect to the new manager without any manual
     * intervention.
     * @throws SQLException 
     * 
     */
    @Test(timeout = 60000)
    public void tableManagerMigration() throws SQLException {

        statements[1].executeUpdate("MIGRATE TABLEMANAGER test");

        /*
         * Test that the new Table Manager can be found.
         */
        statements[1].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");

        /*
         * Test that the old Table Manager is no longer accessible, and that the reference can be updated.
         */
        statements[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");
    }

    /**
     * Tests that when the Table Manager is migrated another database instance is able to migrate back to the original instance without error.
     * @throws SQLException 
     * 
     */
    @Test(timeout = 60000)
    public void tableManagerDoubleMigration() throws SQLException {

        statements[1].executeUpdate("MIGRATE TABLEMANAGER test");

        /*
         * Test that the new Table Manager can be found.
         */
        statements[1].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");

        statements[0].executeUpdate("MIGRATE TABLEMANAGER test");

        /*
         * Test that the new Table Manager can be found.
         */
        statements[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");
        statements[1].executeUpdate("INSERT INTO TEST VALUES(6, 'helloagainagainagain');");

    }

    /**
     * Tests that when migration fails when an incorrect table name is given.
     * @throws SQLException 
     */
    @Test(expected = SQLException.class, timeout = 60000)
    public void tableManagerMigrationFail() throws SQLException {

        statements[1].executeUpdate("MIGRATE TABLEMANAGER testy");
    }

    /**
     * Tests that when the System Table is migrated another database instance is able to connect to the new manager without any manual
     * intervention.
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void systemTableMigration() throws SQLException {

        statements[1].executeUpdate("MIGRATE SYSTEMTABLE");

        statements[2].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        statements[2].execute("SELECT * FROM TEST2;");
    }

    /**
     * Tests that when the System Table is migrated another database instance and back to the original instance that it still works.
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void doubleSystemTableMigration() throws SQLException {

        statements[1].executeUpdate("MIGRATE SYSTEMTABLE");

        statements[2].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        statements[2].execute("SELECT * FROM TEST2;");

        statements[0].execute("MIGRATE SYSTEMTABLE");

        statements[2].executeUpdate("CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        statements[2].execute("SELECT * FROM TEST3;");

    }

    @Test(timeout = 60000)
    public void systemTableFailure() throws InterruptedException, SQLException {

        db_processes[0].destroy();
        statements[0].close();

        Thread.sleep(10000);

        statements[1].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
    }

    @Test(timeout = 60000)
    public void firstMachineDisconnect() throws InterruptedException, SQLException {

        statements[0].close();
        drivers[0].getConnection().close();

        Thread.sleep(10000);

        statements[1].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
    }
}
