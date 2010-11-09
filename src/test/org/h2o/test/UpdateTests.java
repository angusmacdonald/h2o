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
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Assert;

import org.h2o.autonomic.settings.TestingSettings;
import org.h2o.db.manager.PersistentSystemTable;
import org.junit.Test;

/**
 * Tests that check the ability of the system to propagate updates, both in cases where the update can be committed, and in cases where it
 * must be aborted.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class UpdateTests extends TestBase {

    /**
     * Test that an insert is rolled back correctly when one of the database instances fails.
     */
    @Test
    public void testRollbackCapability() {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("This wasn't even the interesting part of the test.");
        }

        TestingSettings.IS_TESTING_PRE_PREPARE_FAILURE = true;

        try {
            sa.execute("INSERT INTO TEST VALUES(3, 'HAHAAHAHAHA');");

            fail("Expected failure.");
        }
        catch (final SQLException e) {
            // e.printStackTrace();
        }
    }

    /**
     * Tests that the database throws an exception when an instance fails on the commit operation.
     * 
     * @throws InterruptedException
     */
    @Test
    public void failureOnCommit() throws InterruptedException {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            fail("This wasn't even the interesting part of the test.");
        }

        TestingSettings.IS_TESTING_PRE_COMMIT_FAILURE = true;

        try {
            sa.execute("INSERT INTO TEST VALUES(3, 'HAHAAHAHAHA');");

            fail("Expected failure.");
        }
        catch (final SQLException e) {
            // e.printStackTrace();
        }

        try {
            /*
             * Start up the failed connection again so that the generic tearDown stuff works.
             */
            cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Tests that the DELETE FROM command synchronously updates all replicas.
     */
    @Test
    public void deleteFromCommand() {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            fail("This wasn't even the interesting part of the test.");
        }

        try {
            sa.execute("DELETE FROM TEST WHERE ID=1;");

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Unexpected failure executing DELETE FROM.");
        }

        /*
         * Now check that each replica sees the result of the deletion.
         */

        final int[] pKey = {2};
        final String[] secondCol = {"World"};

        try {
            sa.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sa.getResultSet());

            sb.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sb.getResultSet());
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Validation of results failed.");
        }
    }

    /**
     * Tests that the UPDATE command synchronously updates all replicas.
     */
    @Test
    public void updateCommand() {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            fail("This wasn't even the interesting part of the test.");
        }

        try {
            sa.execute("UPDATE TEST SET ID=3, NAME='TESTING' WHERE ID=2;");

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Unexpected failure executing UPDATE.");
        }

        /*
         * Now check that each replica sees the result of the deletion.
         */

        final int[] pKey = {1, 3};
        final String[] secondCol = {"Hello", "TESTING"};

        try {
            sa.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sa.getResultSet());

            sb.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sb.getResultSet());
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Validation of results failed.");
        }
    }

    /**
     * Tests that when multiple updates happen on separate tables the system handles them correctly.
     */
    @Test
    public void testMultiTableInserts() {

        try {
            sb.execute("CREATE REPLICA TEST");
            sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
            sa.execute("INSERT INTO TEST2 VALUES(4, 'Hi');");
            sa.execute("INSERT INTO TEST2 VALUES(5, 'Ho');");
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("This wasn't even the interesting part of the test.");
        }

        try {
            sa.execute("INSERT INTO TEST2 VALUES(3, 'HAHAAHAHAHA'); INSERT INTO TEST VALUES(3, 'HAHAAHAHAHA');");

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Expected success.");
        }

        final int[] pKey = {1, 2, 3};
        final String[] secondCol = {"Hello", "World", "HAHAAHAHAHA"};

        try {
            sa.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sa.getResultSet());

            sb.execute("SELECT * FROM TEST ORDER BY ID");
            validateResults(pKey, secondCol, sb.getResultSet());
        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("Validation of results failed.");
        }
    }

    /**
     * Tests the case of multiple database instances attempting to access a table at the same time. Exclusive access should be ensured
     * during the period of writes.
     * 
     * <p>
     * Numerous entries should cause failure, because of the lock contention.
     */
    @Test
    public void testConcurrentQueriesCompetingUpdates() {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("This wasn't even the interesting part of the test.");
        }

        final int entries = 100;
        final ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
        final ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
        new Thread(cta).start();

        ctb.run();

        final boolean result = ctb.successful;

        assertFalse(result);
    }

    /**
     * Tests the case of multiple database instances attempting to access a table at the same time. Only one thread is writing to the table
     * so all queries should run as expected.
     * 
     * <p>
     * Numerous entries should cause failure, because of the lock contention.
     */
    @Test
    public void testConcurrentQueriesNonCompeting() {

        try {
            sb.execute("CREATE REPLICA TEST");
        }
        catch (final SQLException e1) {
            e1.printStackTrace();
            fail("This wasn't even the interesting part of the test.");
        }

        final int entries = 100;
        final ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, false);
        final ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, false);
        new Thread(cta).start();
        ctb.run();

        Assert.assertTrue(ctb.successful);
    }

}
