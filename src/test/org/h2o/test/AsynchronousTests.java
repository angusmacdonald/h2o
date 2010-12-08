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

import java.io.IOException;
import java.sql.SQLException;

import org.h2.table.Table;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.id.DatabaseID;
import org.h2o.test.fixture.MultiProcessTestBase;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Class which tests the asynchronous query functionality of H2O.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class AsynchronousTests extends MultiProcessTestBase {

    /**
     * Tests that an update can complete with only two machines.
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws IOException 
     */
    @Test(timeout = 35000)
    public void basicAsynchronousUpdate() throws InterruptedException, SQLException, IOException {

        final String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

        killDatabase(2);

        sleep(5000);

        delayQueryCommit(2);

        startDatabase(2);

        sleep("About to create recreate connections to the newly restarted database.", 2000);

        createConnectionsToDatabase(2);

        executeUpdateOnNthMachine(create1, 0);

        sleep(1000);

        /*
         * Create test table.
         */
        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        final String createReplica = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(createReplica, 1);
        executeUpdateOnNthMachine(createReplica, 2);

        sleep("About to begin test.\n\n\n\n", 3000);

        final String update = "INSERT INTO TEST VALUES(3, 'Third');";

        executeUpdateOnNthMachine(update, 0);

        assertTestTableExistsLocally(connections[0], 3);
        assertTestTableExistsLocally(connections[1], 3);
        assertTestTableExists(connections[2], 3, false);
    }

    /**
     * Tests that an inactive replica is correctly not called when an update failed.
     * 
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void inactiveReplicaRecognisedOnRestart() throws InterruptedException, SQLException, IOException {

        final String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

        killDatabase(2);

        sleep(5000);

        delayQueryCommit(2);

        startDatabase(2);

        sleep("About to create recreate connections to the newly restarted database.", 2000);

        createConnectionsToDatabase(2);

        executeUpdateOnNthMachine(create1, 1);

        sleep(1000);

        /*
         * Create test table.
         */
        assertTestTableExists(2, 1);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        final String createReplica = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(createReplica, 0);
        executeUpdateOnNthMachine(createReplica, 2);

        sleep("About to begin test.\n\n\n\n", 3000);

        final String update = "INSERT INTO TEST VALUES(3, 'Third');";

        executeUpdateOnNthMachine(update, 1);

        assertTestTableExistsLocally(connections[0], 3);
        assertTestTableExistsLocally(connections[1], 3);

        sleep(2000);

        killDatabase(1);

        sleep(5000);

        sleep("Wait for database to startup and reconnect.", 10000);

        assertTestTableExists(connections[2], 3, false);
    }

    /**
     * Tests that an update will eventually commit on the third machine after the other transaction has completed.
     * 
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void asynchronousUpdateEventuallyCommitted() throws InterruptedException, SQLException, IOException {

        final String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

        killDatabase(2);

        sleep(5000);

        delayQueryCommit(2);

        startDatabase(2);

        sleep("About to create recreate connections to the newly restarted database.", 2000);

        createConnectionsToDatabase(2);

        executeUpdateOnNthMachine(create1, 0);

        sleep(1000);

        /*
         * Create test table.
         */
        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        final String createReplica = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(createReplica, 1);
        executeUpdateOnNthMachine(createReplica, 2);

        sleep("About to begin test.\n\n\n\n", 3000);

        final String update = "INSERT INTO TEST VALUES(3, 'Third');";

        executeUpdateOnNthMachine(update, 0);

        assertTestTableExistsLocally(connections[0], 3);
        assertTestTableExistsLocally(connections[1], 3);

        Thread.sleep(11000);
        assertTestTableExistsLocally(connections[2], 3);
    }

    /**
     * Tests that if another transaction comes along, the first one will fail.
     * 
     * 
     * @throws InterruptedException
     * @throws SQLException 
     * @throws IOException 
     */
    @Test(timeout = 60000)
    public void anotherTransactionIntervenes() throws InterruptedException, SQLException, IOException {

        final String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " + "INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

        killDatabase(2);

        sleep(5000);

        delayQueryCommit(2);

        startDatabase(2);

        sleep("About to create recreate connections to the newly restarted database.", 2000);

        createConnectionsToDatabase(2);

        executeUpdateOnNthMachine(create1, 0);

        sleep(1000);

        /*
         * Create test table.
         */
        assertTestTableExists(2, 0);
        assertMetaDataExists(connections[0], 1);

        sleep(2000);

        final String createReplica = "CREATE REPLICA TEST;";
        executeUpdateOnNthMachine(createReplica, 1);
        executeUpdateOnNthMachine(createReplica, 2);

        sleep("About to begin test.\n\n\n\n", 3000);

        final String update = "INSERT INTO TEST VALUES(3, 'Third');";

        executeUpdateOnNthMachine(update, 0);

        assertTestTableExistsLocally(connections[0], 3);
        assertTestTableExistsLocally(connections[1], 3);

        Thread.sleep(5000);

        final String update2 = "INSERT INTO TEST VALUES(4, 'Fourth');";

        executeUpdateOnNthMachine(update2, 0);

        Thread.sleep(10000);

        try {
            assertTestTableExistsLocally(connections[2], 4);
            fail("Expected an exception to be thrown because this replica is now inactive.");
        }
        catch (final Exception e) {
            //Expected.
        }

        assertTestTableExistsLocally(connections[0], 4);
        assertTestTableExistsLocally(connections[1], 4);
    }

    /**
     * Called from within the H2O codebase to pause a thread artificially (simulating a query taking a while to execute).
     * 
     * @param table
     * @param databaseSettings
     * @param dbID
     * @param query
     */
    public static void pauseThreadIfTestingAsynchronousUpdates(final Table table, final Settings databaseSettings, final DatabaseID dbID, final String query) {

        if (query == null || !query.contains("INSERT INTO TEST VALUES(3, 'Third')")) { return; }

        if (databaseSettings == null) { return; }

        if (!table.getName().contains("TEST")) { return; }

        final boolean delay = Boolean.parseBoolean(databaseSettings.get("DELAY_QUERY_COMMIT"));

        if (delay) {
            try {
                ErrorHandling.errorNoEvent("Delay " + dbID.getURL() + ": " + query);

                Thread.sleep(10000);
            }
            catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
