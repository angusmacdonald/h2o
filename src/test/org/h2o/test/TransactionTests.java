/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.jdbc.JdbcConnection;
import org.h2o.autonomic.settings.TestingSettings;
import org.h2o.db.manager.PersistentSystemTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that check H2O's ability to do distributed transactions such as inserts over multiple replicas.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TransactionTests {

    Connection ca = null;

    Connection cb = null;

    Statement sa = null;

    Statement sb = null;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {

        TestingSettings.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
        // PersistentSystemTable.USERNAME = "sa";
        // PersistentSystemTable.PASSWORD = "sa";

        org.h2.Driver.load();

        ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

        sa = ca.createStatement();
        sb = cb.createStatement();

        sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
        sa.execute("INSERT INTO TEST VALUES(2, 'World');");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() {

        // try{
        // if (!sa.isClosed() && !ca.isClosed()) sa.execute("DROP TABLE IF EXISTS TEST, TEST2");
        // if (!sb.isClosed() && !cb.isClosed()) sb.execute("DROP TABLE IF EXISTS TEST, TEST2");
        //
        // if (!sa.isClosed() && !ca.isClosed()) sa.execute("DROP SCHEMA IF EXISTS SCHEMA2");
        // if (!sb.isClosed() && !cb.isClosed()) sb.execute("DROP SCHEMA IF EXISTS SCHEMA2");
        //
        // if (!sa.isClosed() && !ca.isClosed()) sa.close();
        // if (!sb.isClosed() && !cb.isClosed()) sb.close();
        //
        // if (!ca.isClosed()) ca.close();
        // if (!cb.isClosed()) cb.close();
        //
        // } catch (Exception e){
        // e.printStackTrace();
        // fail("Connections aren't bein closed correctly.");
        // }
    }

    /**
     * Tests that where two replicas exist, an insert statement updates both.
     */
    @Test
    public void updateMultipleReplicas() {

        try {
            sb.execute("CREATE REPLICA TEST");

            if (sb.getUpdateCount() != 0) {
                fail("Expected update count to be '0'");
            }

            sb.execute("INSERT INTO TEST VALUES(3, 'Quite');");

            /*
             * Check that the local copy has only two entries.
             */
            sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

            final int[] pKey = {1, 2, 3};
            final String[] secondCol = {"Hello", "World", "Quite"};

            validateResults(pKey, secondCol, sb.getResultSet());

            /*
             * Check that the primary copy has three entries.
             */
            sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); // Now query on first machine (which should have one extra row).

            validateResults(pKey, secondCol, sb.getResultSet());

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");
        }
    }

    /**
     * Checks that the 2PC protocol works for two distributed operations.
     */
    @Test
    public void localTwoPhase() {

        try {
            ca.setAutoCommit(false);
            sb.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR)");
            sa.execute("CREATE LINKED TABLE IF NOT EXISTS PUBLIC.TEST2('org.h2.Driver', 'jdbc:h2:mem:two', 'sa', 'sa', 'PUBLIC.TEST2');");
            cb.setAutoCommit(false);

            sa.execute("INSERT INTO TEST VALUES(4, 'Hello')");
            sa.execute("INSERT INTO TEST2 VALUES(4, 'Hello')");

            sa.execute("PREPARE COMMIT LITTLE_TEST_TRANSACTION");
            int result = sa.getUpdateCount();

            if (result == 0) {
                sa.execute("COMMIT TRANSACTION LITTLE_TEST_TRANSACTION");
            }

            result = sa.getUpdateCount();

            if (result != 0) {
                fail("Expected this to pass.");
            }

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");
        }
    }

    /**
     * Checks that, on failure, the 2PC protocol rolls back every distributed operation.
     */
    @Test
    public void localTwoPhaseFail() {

        try {
            ca.setAutoCommit(false);
            sb.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR)");
            sa.execute("CREATE LINKED TABLE IF NOT EXISTS PUBLIC.TEST2('org.h2.Driver', 'jdbc:h2:mem:two', 'sa', 'sa', 'PUBLIC.TEST2');");

            cb.setAutoCommit(false);

            sa.execute("INSERT INTO TEST VALUES(4, 'Hello')");
            sa.execute("INSERT INTO TEST2 VALUES(4, 'Hello')");

            ((JdbcConnection) cb).setPowerOffCount(1);
            try {
                cb.createStatement().execute("SET WRITE_DELAY 0");
                cb.createStatement().execute("CREATE TABLE TEST_A(ID INT)");
                fail("should be crashed already");
            }
            catch (final SQLException e) {
                // expected
            }

            sa.execute("PREPARE COMMIT XID_TEST_TRANSACTION_WITH_LONG_NAME");
            int result = sa.getUpdateCount();

            if (result == 0) {
                fail("Prepare should return with a failure at this point.");
            }

            result = sa.getUpdateCount();

            if (result != 0) {
                fail("Expected this to pass.");
            }

        }
        catch (final SQLException e) {
            e.printStackTrace();
            fail("An Unexpected SQLException was thrown.");
        }
        finally {
            try {
                cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            }
            catch (final SQLException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Utility method which checks that the results of a test query match up to the set of expected values. The 'TEST' class is being used
     * in these tests so the primary keys (int) and names (varchar/string) are required to check the validity of the resultset.
     * 
     * @param key
     *            The set of expected primary keys.
     * @param secondCol
     *            The set of expected names.
     * @param rs
     *            The results actually returned.
     * @throws SQLException
     */
    public void validateResults(final int[] pKey, final String[] secondCol, final ResultSet rs) throws SQLException {

        if (rs == null) {
            fail("Resultset was null. Probably an incorrectly set test.");
        }

        for (int i = 0; i < pKey.length; i++) {
            if (rs.next()) {
                assertEquals(pKey[i], rs.getInt(1));
                assertEquals(secondCol[i], rs.getString(2));
            }
            else {
                fail("Expected an entry here.");
            }
        }

        if (rs.next()) {
            fail("Too many entries.");
        }

        rs.close();
    }
}
