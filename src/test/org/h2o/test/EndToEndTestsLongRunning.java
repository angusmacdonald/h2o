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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTestsLongRunning extends EndToEndTests {

    /**
     * A generalised version of {@link #concurrentUpdates()} with multiple threads and multiple values being inserted.
     * 
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void multipleThreads() throws SQLException, IOException {

        Diagnostic.trace();

        final int number_of_values = 20;
        final int number_of_threads = 20;

        createWithAutoCommit();

        final ExecutorService pool = Executors.newFixedThreadPool(number_of_threads);
        final Semaphore sync = new Semaphore(1 - number_of_threads);

        for (int i = 0; i < number_of_threads; i++) {

            final int j = i;

            pool.execute(new UpdateThread(number_of_values, j * number_of_values, 1000, sync));
        }

        waitForThreads(sync);
        shutdown();

        startup();
        assertDataIsPresent(number_of_values * number_of_threads);
    }
}
