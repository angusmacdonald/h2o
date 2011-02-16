/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.h2o.db.manager.recovery.LocatorException;
import org.h2o.test.fixture.MultiProcessTestBase;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Class which conducts tests on <i>n</i> in-memory databases running at the same time.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class FailureTests extends MultiProcessTestBase {

    /**
     * Connect to existing Database instance with ST state, but find no ST running.
     * 
     * The sleep time between failure and a new query is so short that it is the instance performing the lookup that should notice and
     * correct the failure. The next test has a longer sleep time that checks that the maintenance mechanism notices failure and reacts.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void systemTableMigrationOnFailureRequestingInstance() throws InterruptedException, SQLException, IOException, LocatorException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(2000);

        /*
         * Create test table.
         */
        executeUpdateOnFirstMachine(sql);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(4000);

        /*
         * Kill off the System Table process.
         */
        killDatabase(findSystemTableInstance());

        sleep(6000); // if 4000, location will be fixed through handlemovedexception, if 8000 fixed through predecessorChange.

        // createConnectionsToDatabases();

        final Statement stat = connections[1].createStatement();
        createSecondTable(stat, "TEST2");
        assertTest2TableExists(connections[1], 2);
    }

    /**
     * Connect to existing Database instance with ST state, but find no ST running.
     * 
     * The sleep time between failure and a new query is long enough that it is maintenance mechanism that should notice and correct the
     * failure. The previous test has a shorter sleep time that checks that the requesting instance notices failure and reacts.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void systemTableMigrationOnFailureMaintenanceMechanism() throws InterruptedException, SQLException, IOException, LocatorException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnFirstMachine(sql);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        /*
         * Kill off the System Table process.
         */
        killDatabase(findSystemTableInstance());

        sleep(8000); // if 4000, location will be fixed through handlemovedexception, if 8000 fixed through predecessorChange.

        // createConnectionsToDatabases();

        final Statement stat = connections[1].createStatement();
        createSecondTable(stat, "TEST2");
        assertTest2TableExists(connections[1], 2);
    }

    /**
     * Test that System Table correctly records the locations of Table Manager meta-data (given the replication factor specified in
     * the settings for the database (currently 2).
     * 
     * @throws InterruptedException
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void tableManagerMetaDataCorrect() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnFirstMachine(sql);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(8000); // maintenance thread should have replicated table manager meta-data.

        assertTableManagerMetaDataExists(connections[0], 2);
    }

    /**
     * Connect to existing Database instance with ST state and TM state, but find no ST or TM running.
     * 
     * The failure is detected by the migration of the system table, as it is also on the failed machine and will recognise that no Table
     * Manager is available when it re-populates its in-memory state.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void tableManagerMigrationOnFailureDetectedBySystemTableMigration() throws InterruptedException, SQLException, IOException, LocatorException {

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnFirstMachine(sql);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        sql = "CREATE REPLICA TEST;";
        executeUpdateOnSecondMachine(sql);

        sleep(3000);

        assertTestTableExistsLocally(connections[1], 2);

        // Kill off the System Table process.
        killDatabase(findSystemTableInstance());

        sleep(15000);

        assertTestTableExists(connections[1], 2, false);
    }

    /**
     * Connect to existing Database instance with ST state and TM state, but find no ST or TM running.
     * 
     * The failure is detected by the migration of the system table, as it is also on the failed machine and will recognise that no Table
     * Manager is available when it re-populates its in-memory state.
     * 
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void tableManagerMigrationOnFailureDetectedByQueryingInstance() throws InterruptedException, SQLException, IOException, LocatorException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnFirstMachine(sql);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        sql = "CREATE REPLICA TEST;";
        executeUpdateOnSecondMachine(sql);

        sleep("Created replica of test.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the System Table process.
         */
        killDatabase(findSystemTableInstance());

        sleep("Killed off system table instance.", 4000);

        assertTestTableExists(connections[1], 2, false);
    }

    /**
     * Database instance with TM running fails, but the system table is somewhere else. Tests that it can recover.
     * 
     * The query which detects the failure is run locally (the next test does it through a linked table.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void tableManagerMigrationOnFailureSystemTableDoesntFail() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnSecondMachine(sql);

        assertTestTableExists(2, 1);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        sql = "CREATE REPLICA TEST;";
        executeUpdateOnFirstMachine(sql);

        sleep(3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the System Table process.
         */
        killDatabase(fullDbName[1]);

        sleep(4000);

        assertTestTableExistsLocally(connections[0], 2);
    }

    /**
     * Database instance with TM running fails, but the system table is somewhere else. Tests that it can recover.
     * 
     * The query which detects the failure is through a linked table.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void tableManagerMigrationOnFailureSystemTableDoesntFailLinkedTable() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnSecondMachine(sql);

        assertTestTableExists(2, 1);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        sql = "CREATE REPLICA TEST;";
        executeUpdateOnFirstMachine(sql);

        sleep(3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the System Table process.
         */
        killDatabase(fullDbName[1]);

        sleep(4000);

        assertTestTableExists(connections[2], 2, false);
    }

    /**
     * Creates tables on every machine.
     * 
     * Kills off the machine with the SYSTEM TABLE and the table TEST.
     * 
     * Sleeps.
     * 
     * Restarts the machine that was killed off.
     * 
     * Sleeps.
     * 
     * Kills the database with TEST3.
     * 
     * Sleeps.
     * 
     * Queries every table to ensure they are still accessible.
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void multipleFailures() throws InterruptedException, SQLException, IOException, LocatorException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);
        executeUpdateOnNthMachine(create3, 1);

        sleep("Wait for create replica commands to execute.", 10000);

        assertTestTableExistsLocally(connections[1], 2);
        assertTest2TableExists(connections[1], 2);
        assertTest3TableExists(connections[1], 2);

        /*
         * Kill off the System Table process.
         */
        killDatabase(findSystemTableInstance());

        sleep("Killed off System Table database.", 15000);

        assertTestTableExistsLocally(connections[1], 2);

        startDatabase(0);
        createConnectionsToDatabase(0);
        // Database 0 tries to replicate to database 2, but database 2 is killed off before this can happen.
        sleep("About to kill off third database instance.", 2000);
        killDatabase(2);
        sleep("About to test accessibility of test tables.", 4000);

        assertTestTableExistsLocally(connections[1], 2);
        assertTest2TableExists(connections[1], 2);
        assertTest3TableExists(connections[1], 2);
    }

    /**
     * Creates tables on every machine.
     * 
     * Kills off the machine with the SYSTEM TABLE and the table TEST.
     * 
     * Sleeps.
     * 
     * Restarts the machine that was killed off.
     * 
     * Sleeps.
     * 
     * Kills the database with TEST3.
     * 
     * Sleeps.
     * 
     * Queries every table to ensure they are still accessible.
     * @throws SQLException 
     * @throws LocatorException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void multipleSTFailures() throws InterruptedException, SQLException, IOException, LocatorException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        executeUpdateOnNthMachine(create1, 2);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        executeUpdateOnNthMachine(create2, 2);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);
        executeUpdateOnNthMachine(create3, 1);

        sleep("Waiting for create replica commands to execute.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        final Set<String> activeConnections = new HashSet<String>();
        for (final Connection c : connections) {
            activeConnections.add(c.getMetaData().getURL());
        }

        /*
         * Kill off the System Table process.
         */
        String st = findSystemTableInstance();
        activeConnections.remove(st);
        killDatabase(st);

        sleep("Killed off System Table database.", 15000);

        st = findSystemTableInstance();
        activeConnections.remove(st);
        killDatabase(st);

        sleep("Killed of another System Table database.", 15000);

        Connection activeConnection = null;
        for (final Connection c : connections) {
            if (activeConnections.contains(c.getMetaData().getURL())) {
                activeConnection = c;
                break;
            }
        }

        if (activeConnection != null) {
            Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "Active connection to use: " + activeConnection.getMetaData().getURL());
        }
        // assertTrue(assertTestTableExists(activeConnection, 2));
        assertTest2TableExists(activeConnection, 2);
        assertTest3TableExists(activeConnection, 2);
    }

    /**
     * Kill off two database instances holding table managers and try to access the tables. There are replicas of these tables on the
     * remaining active machine, but the third table cannot be accessed because its table manager state wasn't sufficiently replicated.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void killOffNonSTMachines() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);

        sleep("Wait for create replica commands to execute.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the non-system table instances.
         */
        killDatabase(1);
        killDatabase(2);
        sleep("Killed off two databases.", 15000);

        assertTestTableExistsLocally(connections[0], 2);
        assertTest2TableExists(connections[0], 2);

        try {
            assertTest3TableExists(connections[0], 2);
        }
        catch (final SQLException e1) {
            // expected.
        }
    }

    /**
     * Same as {@link #killOffNonSTMachines()} but this time the third table should be accessible. There is a gap between killing off
     * machines that should allow it to re-replicate its state.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void killOffNonSTMachines2() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);

        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);

        sleep("Wait for create replica commands to execute.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the non-system table instances.
         */
        killDatabase(2);
        sleep("Waiting to kill off second database.", 10000);
        killDatabase(1);
        sleep("Killed off two databases.", 15000);

        assertTestTableExistsLocally(connections[0], 2);
        assertTest2TableExists(connections[0], 2);
        assertTest3TableExists(connections[0], 2);
    }

    /**
     * Kills of a DB instance, then queries a table that requires the table manager to be moved. Then restarts the old instance and checks
     * that it can access the table through the new table manager.
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void killOffTMqueryThenRestartMachine() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);

        sleep("Wait for create replica commands to execute.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the non-system table instances.
         */
        killDatabase(2);
        sleep("Killed off database.", 15000);
        assertTest3TableExists(connections[0], 2);

        startDatabase(2);
        sleep("Restarted old database.", 10000);
        connections[2] = createConnectionToDatabase(fullDbName[2]);
        assertTest3TableExists(connections[2], 2);

        // Query through restarted database.
    }

    /**
     * Kills of a DB instance, then queries a table that requires the table manager to be moved. Then restarts the old instance and checks
     * that it can access the table through the new table manager.
     * 
     * Then the TM is killed off again (along with the whole database instance) and the table is queried again.
     * @throws SQLException 
     */
    @Test(timeout = 60000)
    public void killOffTMqueryThenRestartMachineTwice() throws InterruptedException, SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";
        String create2 = "CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST2 VALUES(4, 'Meh'); INSERT INTO TEST2 VALUES(5, 'Heh');";
        String create3 = "CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST3 VALUES(4, 'Clouds'); INSERT INTO TEST3 VALUES(5, 'Rainbows');";

        sleep(1000);
        /*
         * Create test table.
         */
        executeUpdateOnNthMachine(create1, 0);
        executeUpdateOnNthMachine(create2, 1);

        executeUpdateOnNthMachine(create3, 2);

        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 3);

        sleep(2000);

        create1 = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(create1, 1);
        create2 = "CREATE REPLICA TEST2";
        executeUpdateOnNthMachine(create2, 0);
        create3 = "CREATE REPLICA TEST3";
        executeUpdateOnNthMachine(create3, 0);

        sleep("Wait for create replica commands to execute.", 3000);

        assertTestTableExistsLocally(connections[1], 2);

        /*
         * Kill off the non-system table instances.
         */
        killDatabase(2);
        sleep("Killed off database.", 15000);
        assertTest3TableExists(connections[0], 2);

        startDatabase(2);
        sleep("Restarted old database.", 10000);
        connections[2] = createConnectionToDatabase(fullDbName[2]);
        assertTest3TableExists(connections[2], 2);

        killDatabase(0);
        sleep("Killed off database.", 5000);
        assertTest3TableExists(connections[2], 2);

        startDatabase(0);
        sleep("Restarted old database.", 5000);
        connections[0] = createConnectionToDatabase(fullDbName[0]);
        assertTest3TableExists(connections[0], 2);

        // Query through restarted database.
    }
}
