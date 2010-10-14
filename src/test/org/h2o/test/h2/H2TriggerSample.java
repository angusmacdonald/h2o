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
		
		if ( config.memory ) {
			return;
		}
	}
	
	@After
	public void tearDown() throws SQLException {
		
		ls.setRunning(false);
		while ( !ls.isFinished() ) {
		}
		;
		
		DeleteDbFiles.execute("data\\test\\", "test", true);
		
	}
	
	@Test
	public void triggerTest() throws SQLException, ClassNotFoundException {
		DeleteDbFiles.execute("data\\test\\", "test", true);
		
		config = new TestAll();
		
		if ( config.memory ) {
			return;
		}
		
		Connection conn = getConnection("test");
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE INVOICE(ID INT PRIMARY KEY, AMOUNT DECIMAL)");
		stat.execute("CREATE TABLE INVOICE_SUM(AMOUNT DECIMAL)");
		stat.execute("INSERT INTO INVOICE_SUM VALUES(0.0)");
		stat.execute("CREATE TRIGGER INV_INS AFTER INSERT ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
		stat.execute("CREATE TRIGGER INV_UPD AFTER UPDATE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
		stat.execute("CREATE TRIGGER INV_DEL AFTER DELETE ON INVOICE FOR EACH ROW CALL \"org.h2.samples.TriggerSample$MyTrigger\" ");
		
		stat.execute("INSERT INTO INVOICE VALUES(1, 10.0)");
		stat.execute("INSERT INTO INVOICE VALUES(2, 19.95)");
		// stat.execute("UPDATE INVOICE SET AMOUNT=20.0 WHERE ID=2");
		// stat.execute("DELETE FROM INVOICE WHERE ID=1");
		
		ResultSet rs;
		rs = stat.executeQuery("SELECT AMOUNT FROM INVOICE_SUM");
		rs.next();
		assertEquals(29.95, Double.parseDouble("" + rs.getBigDecimal(1)), 0);
		conn.close();
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
		public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) {
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
		public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
			BigDecimal diff = null;
			if ( newRow != null ) {
				diff = (BigDecimal) newRow[1];
			}
			if ( oldRow != null ) {
				BigDecimal m = (BigDecimal) oldRow[1];
				diff = diff == null ? m.negate() : diff.subtract(m);
			}
			PreparedStatement prep = conn.prepareStatement("UPDATE INVOICE_SUM SET AMOUNT=AMOUNT+?");
			prep.setBigDecimal(1, diff);
			prep.execute();
		}
	}
	
}
