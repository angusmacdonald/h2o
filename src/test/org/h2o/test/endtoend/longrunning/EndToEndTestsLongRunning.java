/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test.endtoend.longrunning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.h2o.test.DatabaseType;
import org.h2o.test.endtoend.normal.EndToEndTestDriver;
import org.h2o.test.endtoend.normal.EndToEndTestsCommon;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTestsLongRunning extends EndToEndTestsCommon {

    @Override
    public DatabaseType getDatabaseType() {

        return DatabaseType.DISK;
    }

    /**
     * A generalised version of {@link #concurrentUpdates()} with multiple threads each inserting multiple values via its own connection.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void multipleThreads1() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final int number_of_values = 5;
        final int number_of_threads = 5;
        final int delay = 5000;

        multipleThreads(number_of_values, number_of_threads, delay);
    }

    /**
     * A generalised version of {@link #concurrentUpdates()} with multiple threads each inserting multiple values via its own connection.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException 
     */
    @Test
    public void multipleThreads2() throws SQLException, IOException, UnknownPlatformException {

        Diagnostic.trace();

        final int number_of_values = 20;
        final int number_of_threads = 20;
        final int delay = 0;

        multipleThreads(number_of_values, number_of_threads, delay);
    }

    private void multipleThreads(final int number_of_values, final int number_of_threads, final int delay) throws SQLException, IOException, UnknownPlatformException {

        final EndToEndTestDriver driver1 = makeSpecificTestDriver();

        driver1.createTable();
        driver1.commit();

        final ExecutorService pool = Executors.newFixedThreadPool(number_of_threads);

        // Initial value of -1 means that main thread waiting on it will be blocked until all the worker threads have signalled it.
        final Semaphore sync = new Semaphore(1 - number_of_threads);

        for (int i = 0; i < number_of_threads; i++) {

            final int j = i;
            final EndToEndTestDriver driver = makeSpecificTestDriver();

            driver.setAutoCommitOff();

            pool.execute(new UpdateThread(driver, number_of_values, j * number_of_values, delay, sync));
        }

        waitForSemaphore(sync);

        shutdown();

        try {
            Thread.sleep(10000);
        }
        catch (final InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        startup();

        final EndToEndTestDriver driver2 = makeSpecificTestDriver();
        driver2.assertDataIsCorrect(number_of_values * number_of_threads);
    }
}
