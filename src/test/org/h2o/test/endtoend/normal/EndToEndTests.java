/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database.                       *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/
package org.h2o.test.endtoend.normal;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import org.h2o.test.endtoend.fixture.EndToEndConnectionDriver;
import org.h2o.test.endtoend.fixture.EndToEndTestsCommon;
import org.junit.Test;

import uk.ac.standrews.cs.nds.madface.exceptions.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;

import com.mindbright.ssh2.SSH2Exception;

/**
 * User-oriented tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public abstract class EndToEndTests extends EndToEndTestsCommon {

    /**
      * Checks that a new database can be created, data inserted and read back.
      *
      * @throws SQLException if the test fails
      * @throws IOException if the test fails
      */
    @Test
    public void simpleLifeCycle() throws SQLException, IOException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver = makeSpecificConnectionDriver();

        driver.setAutoCommitOn();
        driver.setNoDelay();

        driver.createTable();
        driver.insertOneRow();
        driver.assertOneRowIsPresent();
    }

    /**
     * Checks that data can be inserted during one instantiation of a database and read in another.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void persistence() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOn();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertOneRow();

        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertOneRowIsPresent();
    }

    /**
     * Checks that data that has been inserted but not committed is visible within the same transaction.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     */
    @Test
    public void updateVisibleWithinTransaction() throws SQLException, IOException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver = makeSpecificConnectionDriver();

        driver.setAutoCommitOff();
        driver.setNoDelay();

        driver.createTable();
        driver.insertOneRow();
        driver.assertOneRowIsPresent();
    }

    /**
     * Checks that data that has been inserted is correctly rolled back when auto-commit is disabled and there is no explicit commit.
     * A table is created and populated in the first instantiation of the database. The second instantiation tries to access the table,
     * which should fail since the transaction creating it did not commit.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void rollbackWithoutAutoCommit() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertOneRow();

        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertTableIsNotPresent();
    }

    /**
     * Checks that data can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void explicitCommit() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertOneRow();
        driver1.commit();

        // When using an in-memory database, shutdown() breaks the connection to it but doesn't destroy it.
        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertOneRowIsPresent();
    }

    /**
     * Checks that an attempt to recreate an existing table fails as expected.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void tableCantBeCreatedTwice() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOn();
        driver1.setNoDelay();

        driver1.createTable();

        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertTableCantBeRecreated();
    }

    /**
     * Checks that an attempt to recreate an existing table is successful when guarded by "IF NOT EXISTS".
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void createIfNotExists() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOn();
        driver1.setNoDelay();

        driver1.createTable();

        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertCreateIfNotExistsSuccessful();
    }

    /**
     * Checks that a series of values can be inserted during one instantiation of a database and read in another, with auto-commit disabled and using explicit commit.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void multipleInserts() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final int number_of_values = 100;

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.setAutoCommitOff();
        driver1.setNoDelay();

        driver1.createTable();
        driver1.insertRows(number_of_values);
        driver1.commit();

        // When using an in-memory database, shutdown() breaks the connection to it but doesn't destroy it.
        shutdown();
        startup();

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();

        driver2.assertDataIsCorrect(number_of_values);
    }

    /**
     * Checks that updates can be performed concurrently. The test starts two threads, each performing an update to the same table, with an artificial delay
     * to increase the probability of temporal overlap.
     *
     * The test currently fails due to an "unexpected code path" error. When that is fixed the test should be changed to make the update threads retry on
     * error, since it's legitimate for an update to fail due to not being able to obtain locks.
     *
     * @throws SQLException if the test fails
     * @throws IOException if the test fails
     * @throws UnknownPlatformException if the database processes cannot be started due to the local platform being unknown
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Test
    public void concurrentUpdates() throws SQLException, IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        Diagnostic.trace();

        final EndToEndConnectionDriver driver1 = makeSpecificConnectionDriver();

        driver1.createTable();
        driver1.commit();

        // Initial value of -1 means that main thread waiting on it will be blocked until it is signalled twice.
        final Semaphore sync = new Semaphore(-1);

        final EndToEndConnectionDriver driver2 = makeSpecificConnectionDriver();
        final EndToEndConnectionDriver driver3 = makeSpecificConnectionDriver();

        driver2.setAutoCommitOff();
        driver3.setAutoCommitOff();

        new UpdateThread(driver2, 1, 0, 5000, sync).start();
        new UpdateThread(driver3, 1, 1, 5000, sync).start();

        waitForSemaphore(sync);

        shutdown();
        startup();

        final EndToEndConnectionDriver driver4 = makeSpecificConnectionDriver();
        driver4.assertDataIsCorrect(2);
    }
}
