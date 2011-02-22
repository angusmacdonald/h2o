/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.h2o.test.fixture.DiskConnectionDriverFactory;
import org.h2o.test.fixture.DiskTestManager;
import org.h2o.test.fixture.H2OTestBase;
import org.h2o.test.fixture.IDiskConnectionDriverFactory;
import org.h2o.test.fixture.ITestManager;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.madface.exceptions.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

import com.mindbright.ssh2.SSH2Exception;

/**
 * Test for batch updates.
 */
public class H2TestBatchUpdates extends H2OTestBase {

    private static final String COFFEE_UPDATE = "UPDATE TEST SET PRICE=PRICE*20 WHERE TYPE_ID=?";
    private static final String COFFEE_SELECT = "SELECT PRICE FROM TEST WHERE KEY_ID=?";
    private static final String COFFEE_INSERT1 = "INSERT INTO TEST VALUES(9,'COFFEE-9',9.0,5)";
    private static final String COFFEE_DELETE1 = "DELETE FROM TEST WHERE KEY_ID=9";
    private static final String COFFEE_UPDATE1 = "UPDATE TEST SET PRICE=PRICE*20 WHERE TYPE_ID=1";
    private static final String COFFEE_SELECT1 = "SELECT PRICE FROM TEST WHERE KEY_ID>4";
    private static final String COFFEE_UPDATE_SET = "UPDATE TEST SET KEY_ID=?, C_NAME=? WHERE C_NAME=?";
    private static final String COFFEE_SELECT_CONTINUED = "SELECT COUNT(*) FROM TEST WHERE C_NAME='Continue-1'";

    private static final int coffeeSize = 10;
    private static final int coffeeType = 11;

    private Connection connection;
    private Statement statement;
    private PreparedStatement prep;

    private final IDiskConnectionDriverFactory connection_driver_factory = new DiskConnectionDriverFactory();

    private final ITestManager test_manager = new DiskTestManager(1, connection_driver_factory);

    @Override
    public ITestManager getTestManager() {

        return test_manager;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException, SSH2Exception, TimeoutException {

        super.setUp();

        connection = makeConnectionDriver().getConnection();
    }

    @Test(timeout = 60000)
    public void testExecuteCall() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;
        CallableStatement call = null;
        try {
            stat = connection.createStatement();
            stat.execute("CREATE ALIAS updatePrices FOR \"" + getClass().getName() + ".updatePrices\"");
            call = connection.prepareCall("{call updatePrices(?, ?)}");
            call.setString(1, "Hello");
            call.setFloat(2, 1.4f);
            call.addBatch();
            call.setString(1, "World");
            call.setFloat(2, 3.2f);
            call.addBatch();
            final int[] updateCounts = call.executeBatch();
            int total = 0;
            for (final int updateCount : updateCounts) {
                total += updateCount;
            }
            assertEquals(4, total);
        }
        finally {
            closeIfNotNull(call);
            closeIfNotNull(stat);
        }
    }

    /**
     * This method is called by the database.
     *
     * @param message
     *            the message (currently not used)
     * @param f
     *            the float
     * @return the float converted to an int
     */
    public static int updatePrices(final String message, final double f) {

        return (int) f;
    }

    @Test(timeout = 60000)
    public void testException() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = connection.createStatement();
            stat.execute("create table test(id int primary key)");
            prep = connection.prepareStatement("insert into test values(?)");
            for (int i = 0; i < 700; i++) {
                prep.setString(1, "x");
                prep.addBatch();
            }
            try {
                prep.executeBatch();
            }
            catch (final BatchUpdateException e) {
                final PrintStream temp = System.err;
                try {
                    final ByteArrayOutputStream buff = new ByteArrayOutputStream();
                    final PrintStream p = new PrintStream(buff);
                    System.setErr(p);
                }
                finally {
                    System.setErr(temp);
                }
            }
        }
        finally {
            closeIfNotNull(stat);
            closeIfNotNull(prep);
        }
    }

    public void testCoffee() throws SQLException {

        statement = connection.createStatement();
        final DatabaseMetaData meta = connection.getMetaData();
        assertTrue(!meta.supportsBatchUpdates());

        statement.executeUpdate("DROP TABLE IF EXISTS TEST;");
        statement.executeUpdate("CREATE TABLE TEST(KEY_ID INT PRIMARY KEY," + "C_NAME VARCHAR(255),PRICE DECIMAL(20,2),TYPE_ID INT)");
        String newName = null;
        float newPrice = 0;
        int newType = 0;
        prep = connection.prepareStatement("INSERT INTO TEST VALUES(?,?,?,?)");
        int newKey = 1;
        for (int i = 1; i <= coffeeType && newKey <= coffeeSize; i++) {
            for (int j = 1; j <= i && newKey <= coffeeSize; j++) {
                newName = "COFFEE-" + newKey;
                newPrice = newKey + (float) .00;
                newType = i;
                prep.setInt(1, newKey);
                prep.setString(2, newName);
                prep.setFloat(3, newPrice);
                prep.setInt(4, newType);
                prep.execute();
                newKey = newKey + 1;
            }
        }

        testAddBatch01();
        testAddBatch02();
        testClearBatch01();
        testClearBatch02();
        testExecuteBatch01();
        testExecuteBatch02();
        testExecuteBatch03();
        testExecuteBatch04();
        testExecuteBatch05();
        testExecuteBatch06();
        testExecuteBatch07();
        testContinueBatch01();

        connection.close();
    }

    private void testAddBatch01() throws SQLException {

        int i = 0;
        final int[] retValue = {0, 0, 0};
        final String s = COFFEE_UPDATE;
        prep = connection.prepareStatement(s);
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        prep.setInt(1, 4);
        prep.addBatch();
        final int[] updateCount = prep.executeBatch();
        final int updateCountLen = updateCount.length;

        assertEquals(3, updateCountLen);

        final String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=2";
        final String query2 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=3";
        final String query3 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=4";
        ResultSet rs = statement.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = statement.executeQuery(query2);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = statement.executeQuery(query3);
        rs.next();
        retValue[i++] = rs.getInt(1);
        for (int j = 0; j < updateCount.length; j++) {
            assertEquals(updateCount[j], retValue[j]);
        }
    }

    private void testAddBatch02() throws SQLException {

        int i = 0;
        final int[] retValue = {0, 0, 0};
        int updCountLength = 0;
        final String sUpdCoffee = COFFEE_UPDATE1;
        final String sDelCoffee = COFFEE_DELETE1;
        final String sInsCoffee = COFFEE_INSERT1;
        statement.addBatch(sUpdCoffee);
        statement.addBatch(sDelCoffee);
        statement.addBatch(sInsCoffee);
        final int[] updateCount = statement.executeBatch();
        updCountLength = updateCount.length;

        assertEquals(3, updCountLength);

        final String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        final ResultSet rs = statement.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        // 1 as delete Statement will delete only one row
        retValue[i++] = 1;
        // 1 as insert Statement will insert only one row
        retValue[i++] = 1;

        for (int j = 0; j < updateCount.length; j++) {

            assertEquals(updateCount[j], retValue[j]);
        }
    }

    private void testClearBatch01() throws SQLException {

        final String sPrepStmt = COFFEE_UPDATE;
        prep = connection.prepareStatement(sPrepStmt);
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        prep.setInt(1, 4);
        prep.addBatch();
        prep.clearBatch();
        final int[] updateCount = prep.executeBatch();
        final int updCountLength = updateCount.length;
        assertEquals(0, updCountLength);
    }

    private void testClearBatch02() throws SQLException {

        int updCountLength = 0;
        final String sUpdCoffee = COFFEE_UPDATE1;
        final String sInsCoffee = COFFEE_INSERT1;
        final String sDelCoffee = COFFEE_DELETE1;
        statement.addBatch(sUpdCoffee);
        statement.addBatch(sDelCoffee);
        statement.addBatch(sInsCoffee);
        statement.clearBatch();
        final int[] updateCount = statement.executeBatch();
        updCountLength = updateCount.length;

        assertEquals(0, updCountLength);
    }

    private void testExecuteBatch01() throws SQLException {

        int i = 0;
        final int[] retValue = {0, 0, 0};
        int updCountLength = 0;
        final String sPrepStmt = COFFEE_UPDATE;

        // get the PreparedStatement object
        prep = connection.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.addBatch();
        prep.setInt(1, 2);
        prep.addBatch();
        prep.setInt(1, 3);
        prep.addBatch();
        final int[] updateCount = prep.executeBatch();
        updCountLength = updateCount.length;

        assertEquals(3, updCountLength);

        // 1 is the number that is set First for Type Id in Prepared Statement
        final String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        // 2 is the number that is set second for Type id in Prepared Statement
        final String query2 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=2";
        // 3 is the number that is set Third for Type id in Prepared Statement
        final String query3 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=3";
        ResultSet rs = statement.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = statement.executeQuery(query2);
        rs.next();
        retValue[i++] = rs.getInt(1);
        rs = statement.executeQuery(query3);
        rs.next();
        retValue[i++] = rs.getInt(1);

        for (int j = 0; j < updateCount.length; j++) {

            assertEquals(updateCount[j], retValue[j]);
        }
    }

    private void testExecuteBatch02() throws SQLException {

        final String sPrepStmt = COFFEE_UPDATE;

        prep = connection.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.setInt(1, 2);
        prep.setInt(1, 3);
        final int[] updateCount = prep.executeBatch();
        final int updCountLength = updateCount.length;

        assertEquals(0, updCountLength);
    }

    private void testExecuteBatch03() throws SQLException {

        final String sPrepStmt = COFFEE_SELECT;

        prep = connection.prepareStatement(sPrepStmt);
        prep.setInt(1, 1);
        prep.addBatch();
        try {
            prep.executeBatch();
            fail();
        }
        catch (final BatchUpdateException b) {
            // Ignore.
        }
    }

    private void testExecuteBatch04() throws SQLException {

        int i = 0;
        final int[] retValue = {0, 0, 0};
        int updCountLength = 0;
        final String sUpdCoffee = COFFEE_UPDATE1;
        final String sInsCoffee = COFFEE_INSERT1;
        final String sDelCoffee = COFFEE_DELETE1;
        statement.addBatch(sUpdCoffee);
        statement.addBatch(sDelCoffee);
        statement.addBatch(sInsCoffee);
        final int[] updateCount = statement.executeBatch();
        updCountLength = updateCount.length;

        assertEquals(3, updCountLength);

        final String query1 = "SELECT COUNT(*) FROM TEST WHERE TYPE_ID=1";
        final ResultSet rs = statement.executeQuery(query1);
        rs.next();
        retValue[i++] = rs.getInt(1);
        // 1 as Delete Statement will delete only one row
        retValue[i++] = 1;
        // 1 as Insert Statement will insert only one row
        retValue[i++] = 1;
        for (int j = 0; j < updateCount.length; j++) {
            assertEquals(updateCount[j], retValue[j]);
        }
    }

    private void testExecuteBatch05() throws SQLException {

        int updCountLength = 0;
        final int[] updateCount = statement.executeBatch();
        updCountLength = updateCount.length;
        assertEquals(0, updCountLength);
    }

    private void testExecuteBatch06() throws SQLException {

        // Insert a row which is already Present
        final String sInsCoffee = COFFEE_INSERT1;
        final String sDelCoffee = COFFEE_DELETE1;
        statement.addBatch(sInsCoffee);
        statement.addBatch(sInsCoffee);
        statement.addBatch(sDelCoffee);
        try {
            statement.executeBatch();
            fail();
        }
        catch (final BatchUpdateException b) {
            // Ignore.
        }
    }

    private void testExecuteBatch07() throws SQLException {

        final String selectCoffee = COFFEE_SELECT1;
        Statement stmt = null;

        try {
            stmt = connection.createStatement();
            stmt.addBatch(selectCoffee);
            try {
                stmt.executeBatch();
                fail();
            }
            catch (final BatchUpdateException be) {
                // Ignore.
            }
        }
        finally {
            stmt.close();
        }
    }

    private void testContinueBatch01() throws SQLException {

        int[] batchUpdates = {0, 0, 0};
        int buCountLen = 0;
        try {
            final String sPrepStmt = COFFEE_UPDATE_SET;
            prep = connection.prepareStatement(sPrepStmt);
            // Now add a legal update to the batch
            prep.setInt(1, 1);
            prep.setString(2, "Continue-1");
            prep.setString(3, "COFFEE-1");
            prep.addBatch();
            // Now add an illegal update to the batch by
            // forcing a unique constraint violation
            // Try changing the key_id of row 3 to 1.
            prep.setInt(1, 1);
            prep.setString(2, "Invalid");
            prep.setString(3, "COFFEE-3");
            prep.addBatch();
            // Now add a second legal update to the batch
            // which will be processed ONLY if the driver supports
            // continued batch processing according to 6.2.2.3
            // of the J2EE platform spec.
            prep.setInt(1, 2);
            prep.setString(2, "Continue-2");
            prep.setString(3, "COFFEE-2");
            prep.addBatch();
            // The executeBatch() method will result in a
            // BatchUpdateException
            prep.executeBatch();
        }
        catch (final BatchUpdateException b) {
            batchUpdates = b.getUpdateCounts();
            buCountLen = batchUpdates.length;
        }

        if (buCountLen == 3) {

            // Check to see if the third row from the batch was added
            final String query = COFFEE_SELECT_CONTINUED;
            final ResultSet rs = statement.executeQuery(query);
            rs.next();
            final int count = rs.getInt(1);
            rs.close();
            statement.close();
            // Make sure that we have the correct error code for
            // the failed update.
            if (!(batchUpdates[1] == -3 && count == 1)) {
                // ANGUS - the error codes suggest that each batch was executed, but the first set of inserts don't appear after a select
                // query.
                fail("insert failed");
            }
        }
    }
}
