/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.Trigger;
import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * This sample application shows how to use database triggers.
 */
public class H2TriggerSample extends H2TestBase {

    private LocatorServer ls;

    @Before
    public void setUp() throws SQLException {

        DeleteDbFiles.execute("data\\test\\", "test", true);

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        config = new TestAll();

        if (config.memory) { return; }
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {

        ls.setRunning(false);
        while (!ls.isFinished()) {
            Thread.sleep(SHUTDOWN_CHECK_DELAY);
        };

        DeleteDbFiles.execute("data\\test\\", "test", true);
    }

    @Test(timeout = 60000)
    public void triggerTest() throws SQLException, ClassNotFoundException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        DeleteDbFiles.execute("data\\test\\", "test", true);

        config = new TestAll();

        if (config.memory) { return; }

        final Connection conn = getConnection("test");
        final Statement stat = conn.createStatement();
        try {
            stat.execute("CREATE TABLE INVOICE(ID INT PRIMARY KEY, AMOUNT DECIMAL)");
            stat.execute("CREATE TABLE INVOICE_SUM(AMOUNT DECIMAL)");
            stat.execute("INSERT INTO INVOICE_SUM VALUES(0.0)");
            stat.execute("CREATE TRIGGER INV_INS AFTER INSERT ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
            stat.execute("CREATE TRIGGER INV_UPD AFTER UPDATE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
            stat.execute("CREATE TRIGGER INV_DEL AFTER DELETE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");

            stat.execute("INSERT INTO INVOICE VALUES(1, 10.0)");
            stat.execute("INSERT INTO INVOICE VALUES(2, 19.95)");

            ResultSet rs;
            rs = stat.executeQuery("SELECT AMOUNT FROM INVOICE_SUM");
            rs.next();
            assertEquals(29.95, Double.parseDouble("" + rs.getBigDecimal(1)), 0);
        }
        finally {
            stat.close();
            conn.close();
        }

    }

    /**
     * This class is a simple trigger implementation.
     */
    public static class MyTrigger implements Trigger {

        /**
         * Initializes the trigger.
         * 
         * @param conn
         *            a connection to the database
         * @param schemaName
         *            the name of the schema
         * @param triggerName
         *            the name of the trigger used in the CREATE TRIGGER statement
         * @param tableName
         *            the name of the table
         * @param before
         *            whether the fire method is called before or after the operation is performed
         * @param type
         *            the operation type: INSERT, UPDATE, or DELETE
         */
        @Override
        public void init(final Connection conn, final String schemaName, final String triggerName, final String tableName, final boolean before, final int type) {

            // Initializing trigger
        }

        /**
         * This method is called for each triggered action.
         * 
         * @param conn
         *            a connection to the database
         * @param oldRow
         *            the old row, or null if no old row is available (for INSERT)
         * @param newRow
         *            the new row, or null if no new row is available (for DELETE)
         * @throws SQLException
         *             if the operation must be undone
         */
        @Override
        public void fire(final Connection conn, final Object[] oldRow, final Object[] newRow) throws SQLException {

            BigDecimal diff = null;
            if (newRow != null) {
                diff = (BigDecimal) newRow[1];
            }
            if (oldRow != null) {
                final BigDecimal m = (BigDecimal) oldRow[1];
                diff = diff == null ? m.negate() : diff.subtract(m);
            }
            final PreparedStatement prep = conn.prepareStatement("UPDATE INVOICE_SUM SET AMOUNT=AMOUNT+?");
            prep.setBigDecimal(1, diff);
            prep.execute();
        }
    }
}
