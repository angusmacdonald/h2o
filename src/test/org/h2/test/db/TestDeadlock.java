/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;
import org.h2o.autonomic.settings.TestingSettings;

/**
 * Test the deadlock detection mechanism.
 */
public class TestDeadlock extends TestBase {

    /**
     * The first connection.
     */
    Connection c1;

    /**
     * The second connection.
     */
    Connection c2;

    /**
     * The third connection.
     */
    Connection c3;

    private volatile SQLException lastException;

    /**
     * Run just this test.
     * 
     * @param a
     *            ignored
     */
    public static void main(final String[] a) throws Exception {

        TestingSettings.IS_TESTING_H2_TESTS = true;
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {

        deleteDb("deadlock");
        testDiningPhilosophers();
        testLockUpgrade();
        testThreePhilosophers();
        testNoDeadlock();
        deleteDb("deadlock");
    }

    private void initTest() throws SQLException, IOException {

        c1 = getConnection("deadlock");
        c2 = getConnection("deadlock");
        c3 = getConnection("deadlock");
        c1.createStatement().execute("SET LOCK_TIMEOUT 1000");
        c2.createStatement().execute("SET LOCK_TIMEOUT 1000");
        c3.createStatement().execute("SET LOCK_TIMEOUT 1000");
        c1.setAutoCommit(false);
        c2.setAutoCommit(false);
        c3.setAutoCommit(false);
        lastException = null;
    }

    private void end() throws SQLException {

        c1.close();
        c2.close();
        c3.close();
    }

    /**
     * This class wraps exception handling to simplify creating small threads that execute a statement.
     */
    abstract class DoIt extends Thread {

        /**
         * The operation to execute.
         */
        abstract void execute() throws SQLException;

        @Override
        public void run() {

            try {
                execute();
            }
            catch (final SQLException e) {
                catchDeadlock(e);
            }
        }
    }

    /**
     * Add the exception to the list of exceptions.
     * 
     * @param e
     *            the exception
     */
    void catchDeadlock(final SQLException e) {

        if (lastException != null) {
            lastException.setNextException(e);
        }
        else {
            lastException = e;
        }
    }

    private void testNoDeadlock() throws Exception {

        initTest();
        c1.createStatement().execute("CREATE TABLE TEST_A(ID INT PRIMARY KEY)");
        c1.createStatement().execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY)");
        c1.createStatement().execute("CREATE TABLE TEST_C(ID INT PRIMARY KEY)");
        c1.commit();
        c1.createStatement().execute("INSERT INTO TEST_A VALUES(1)");
        c2.createStatement().execute("INSERT INTO TEST_B VALUES(1)");
        c3.createStatement().execute("INSERT INTO TEST_C VALUES(1)");
        final DoIt t2 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c1.createStatement().execute("DELETE FROM TEST_B");
                c1.commit();
            }
        };
        t2.start();
        final DoIt t3 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c2.createStatement().execute("DELETE FROM TEST_C");
                c2.commit();
            }
        };
        t3.start();
        Thread.sleep(500);
        try {
            c3.createStatement().execute("DELETE FROM TEST_C");
            c3.commit();
        }
        catch (final SQLException e) {
            catchDeadlock(e);
        }
        t2.join();
        t3.join();
        if (lastException != null) { throw lastException; }
        c1.commit();
        c2.commit();
        c3.commit();
        c1.createStatement().execute("DROP TABLE TEST_A, TEST_B, TEST_C");
        end();

    }

    private void testThreePhilosophers() throws Exception {

        if (config.mvcc) { return; }
        initTest();
        c1.createStatement().execute("CREATE TABLE TEST_A(ID INT PRIMARY KEY)");
        c1.createStatement().execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY)");
        c1.createStatement().execute("CREATE TABLE TEST_C(ID INT PRIMARY KEY)");
        c1.commit();
        c1.createStatement().execute("INSERT INTO TEST_A VALUES(1)");
        c2.createStatement().execute("INSERT INTO TEST_B VALUES(1)");
        c3.createStatement().execute("INSERT INTO TEST_C VALUES(1)");
        final DoIt t2 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c1.createStatement().execute("DELETE FROM TEST_B");
                c1.commit();
            }
        };
        t2.start();
        final DoIt t3 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c2.createStatement().execute("DELETE FROM TEST_C");
                c2.commit();
            }
        };
        t3.start();
        try {
            c3.createStatement().execute("DELETE FROM TEST_A");
            c3.commit();
        }
        catch (final SQLException e) {
            catchDeadlock(e);
        }
        t2.join();
        t3.join();
        checkDeadlock();
        c1.commit();
        c2.commit();
        c3.commit();
        c1.createStatement().execute("DROP TABLE TEST_A, TEST_B, TEST_C");
        end();
    }

    private void testLockUpgrade() throws Exception {

        if (config.mvcc) { return; }
        initTest();
        c1.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        c1.createStatement().execute("INSERT INTO TEST VALUES(1)");
        c1.commit();
        c1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        c2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        c1.createStatement().executeQuery("SELECT * FROM TEST");
        c2.createStatement().executeQuery("SELECT * FROM TEST");
        final Thread t1 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c1.createStatement().execute("DELETE FROM TEST");
                c1.commit();
            }
        };
        t1.start();
        try {
            c2.createStatement().execute("DELETE FROM TEST");
            c2.commit();
        }
        catch (final SQLException e) {
            catchDeadlock(e);
        }
        t1.join();
        checkDeadlock();
        c1.commit();
        c2.commit();
        c1.createStatement().execute("DROP TABLE TEST");
        end();
    }

    private void testDiningPhilosophers() throws Exception {

        if (config.mvcc) { return; }
        initTest();
        c1.createStatement().execute("CREATE TABLE T1(ID INT)");
        c1.createStatement().execute("CREATE TABLE T2(ID INT)");
        c1.createStatement().execute("INSERT INTO T1 VALUES(1)");
        c2.createStatement().execute("INSERT INTO T2 VALUES(1)");
        final DoIt t1 = new DoIt() {

            @Override
            public void execute() throws SQLException {

                c1.createStatement().execute("INSERT INTO T2 VALUES(2)");
                c1.commit();
            }
        };
        t1.start();
        try {
            c2.createStatement().execute("INSERT INTO T1 VALUES(2)");
        }
        catch (final SQLException e) {
            catchDeadlock(e);
        }
        t1.join();
        checkDeadlock();
        c1.commit();
        c2.commit();
        c1.createStatement().execute("DROP TABLE T1, T2");
        end();
    }

    private void checkDeadlock() throws SQLException {

        if (lastException != null) {
            lastException.printStackTrace();
        }

        assertTrue(lastException != null);
        assertKnownException(lastException);
        assertEquals(ErrorCode.DEADLOCK_1, lastException.getErrorCode());
        final SQLException e2 = lastException.getNextException();
        if (e2 != null) {
            // we have two exception, but there should only be one
            throw e2;
        }
    }

}
