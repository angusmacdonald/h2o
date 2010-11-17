/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test.endtoend.fixture;

import java.util.concurrent.Semaphore;

import org.h2o.test.fixture.H2OTestBase;

/**
 * User-oriented tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public abstract class EndToEndTestsCommon extends H2OTestBase {

    protected EndToEndConnectionDriver makeSpecificConnectionDriver() {

        return (EndToEndConnectionDriver) getTestManager().makeConnectionDriver(0);
    }

    public static class UpdateThread extends Thread {

        private final EndToEndConnectionDriver driver;
        private final int number_of_values;
        private final int starting_value;
        private final long delay;
        private final Semaphore sync;

        public UpdateThread(final EndToEndConnectionDriver driver, final int number_of_values, final int starting_value, final long delay, final Semaphore sync) {

            this.driver = driver;
            this.number_of_values = number_of_values;
            this.starting_value = starting_value;
            this.delay = delay;
            this.sync = sync;
        }

        @Override
        public void run() {

            try {
                driver.setDelay(delay);
                driver.insertRows(number_of_values, starting_value, true);
            }
            finally {
                sync.release();
            }
        }
    };

    protected void waitForSemaphore(final Semaphore sync) {

        while (true) {
            try {
                sync.acquire();
                break;
            }
            catch (final InterruptedException e) {
                // Try again.
            }
        }
    }
}
