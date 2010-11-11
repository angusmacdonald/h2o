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
import java.sql.SQLException;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * User-oriented tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTests extends EndToEndTestsCommon {

    /**
      * Tests whether a new database can be created, data inserted and read back.
      * 
      * @throws SQLException if the test fails
      * @throws IOException if the test fails
      */
    @Test
    public void simpleLifeCycle() throws SQLException, IOException {

        Diagnostic.trace();

        final EndToEndTestDriver driver = makeSpecificTestDriver();

        driver.setAutoCommitOn();
        driver.setNoDelay();

        driver.createTable();
        driver.insertOneRow();
        driver.assertOneRowIsPresent();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     * @throws InterruptedException 
     */
    @Test
    public void persistence() throws SQLException, IOException, UnknownPlatformException, InterruptedException {

        Diagnostic.trace();

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.setAutoCommitOn();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertOneRow();

        shutdown();

        Thread.sleep(10000);
        startup();

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();

        driver2.assertOneRowIsPresent();
    }

    /**
     * Tests whether data that has been inserted but not committed is visible within the same transaction.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void updateVisibleWithinTransaction() throws SQLException, IOException {

        Diagnostic.trace();

        final EndToEndTestDriver driver = makeSpecificTestDriver();

        driver.setAutoCommitOff();
        driver.setNoDelay();

        driver.createTable();
        driver.insertOneRow();
        driver.assertOneRowIsPresent();
    }

    /**
     * Tests whether data that has been inserted is correctly rolled back when auto-commit is disabled and there is no explicit commit.
     * A table is created and populated in the first instantiation of the database. The second instantiation tries to read the data, which should fail.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void rollbackWithoutAutoCommit() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();

        driver1.commit();

        driver1.insertOneRow();

        shutdown();
        startup();

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();

        driver2.assertDataIsNotPresent();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void explicitCommit() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertOneRow();
        driver1.commit();

        shutdown();
        startup();

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();

        driver2.assertOneRowIsPresent();
    }

    /**
     * Tests whether a series of values can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void multipleInserts() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final int number_of_values = 100;

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertRows(number_of_values);
        driver1.commit();

        shutdown();
        startup();

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();

        driver2.assertDataIsCorrect(number_of_values);
    }

    /**
     * Tests whether updates can be performed concurrently. The test starts two threads, each performing an update to the same table, with an artificial delay
     * to increase the probability of temporal overlap.
     * 
     * The test currently fails due to an "unexpected code path" error. When that is fixed the test should be changed to make the update threads retry on
     * error, since it's legitimate for an update to fail due to not being able to obtain locks.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void concurrentUpdates() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.createTable();
        driver1.commit();

        // Initial value of -1 means that main thread waiting on it will be blocked until it is signalled twice.
        final Semaphore sync = new Semaphore(-1);

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();
        final EndToEndTestDriver driver3 = makeSpecificTestDriver();

        driver2.setAutoCommitOff();
        driver3.setAutoCommitOff();

        new UpdateThread(driver2, 1, 0, 5000, sync).start();
        new UpdateThread(driver3, 1, 1, 5000, sync).start();

        waitForThreads(sync);

        shutdown();
        startup();

        final EndToEndTestDriver driver4 = makeSpecificTestDriver();
        driver4.assertDataIsCorrect(2);
    }
}
