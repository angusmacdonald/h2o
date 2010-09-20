/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * This class tests H2O's CREATE REPLICA functionality with regards to its ability to maintain
 * referential integrity by copying indexes for primary keys, etc...
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class IndexTests{

	Connection ca = null;
	Connection cb = null;
	Statement sa = null;
	Statement sb = null;
	private LocatorServer ls;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:mem:one";
		Constants.IS_NON_SM_TEST = true;
		//PersistentSystemTable.USERNAME = "sa";
		//PersistentSystemTable.PASSWORD = "sa";

		TestBase.setUpDescriptorFiles();
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();

		ls.start();

		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

		sa = ca.createStatement();
		sb = cb.createStatement();

		sa.execute("CREATE TABLE Address(id INT NOT NULL, street VARCHAR(255), PRIMARY KEY (id)); " +
				"CREATE TABLE Person(id INT NOT NULL, name VARCHAR(255), address_id INT NOT NULL, PRIMARY KEY (id), FOREIGN KEY (address_id) REFERENCES Address (id));" +
				"INSERT INTO Address VALUES (0, 'Glasgow Road');" +
				"INSERT INTO Address VALUES (1, 'Kinnessburn Terrace');" +
				"INSERT INTO Address VALUES (2, 'Lamond Drive');" +
				"INSERT INTO Address VALUES (3, 'North Street');" +
				"INSERT INTO Address VALUES (4, 'Market Street');" +
				"INSERT INTO Address VALUES (5, 'Hawthorn Avenue');" +
				"INSERT INTO Person VALUES (0, 'Angus Macdonald', 0);" +
				"INSERT INTO Person VALUES (1, 'Alan Dearle', 1);" +
				"INSERT INTO Person VALUES (2, 'Graham Kirby', 2);" +
				"INSERT INTO Person VALUES (3, 'Dharini Balasubramaniam', 2);" +
		"INSERT INTO Person VALUES (4, 'Jon Lewis', 3);");

		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:mem:one";
		//PersistentSystemTable.USERNAME = "sa";
		//PersistentSystemTable.PASSWORD = "sa";


	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		try{ 
			sa.execute("DROP TABLE IF EXISTS Address, Person");
			sb.execute("DROP TABLE IF EXISTS Address, Person");

			sa.close();
			sb.close();

			ca.close();	
			cb.close();	
			Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "END OF LAST TEST (TEAR DOWN 1).");
			TestBase.closeDatabaseCompletely();
		} catch (Exception e){
			e.printStackTrace();
			fail("Connections aren't bein closed correctly.");
		}
		ls.setRunning(false);
		while (!ls.isFinished()){};
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "END OF LAST TEST (TEAR DOWN 2).");
	}

	/**
	 * Check that a tableset ID is correctly given for all new tables, meaning that tables which refer to one another
	 * are given the same ID.
	 * 
	 * <p>This test is also a pretty good indicator of whether constraints are being correctly applied to tables (FOREIGN KEY references etc.),
	 * because the tableset relies on the constraints to know what set to apply to.
	 */
	@Test
	public void checkTableSetID(){
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try{
			/*
			 * These fail because the command which links the two tables doesn't work when query propagation is used on AlterTableAddConstraint.
			 * But if this feature is bypassed another set of tests fail.
			 */ 
			sa.execute("SELECT H2O.H2O_ONE_H2O_TM_TABLE.table_id, table_set FROM H2O.H2O_ONE_H2O_TM_TABLE, H2O.H2O_ONE_H2O_TM_REPLICA " +
					"WHERE H2O.H2O_ONE_H2O_TM_REPLICA.table_id=H2O.H2O_ONE_H2O_TM_TABLE.table_id ORDER BY table_id;");

			ResultSet rs = sa.getResultSet();

			int firstTableSet = -2;
			if (rs.next()){
				firstTableSet = rs.getInt(2);
			}
			if (rs.next()){
				assertEquals(firstTableSet, rs.getInt(2));
			} else {
				fail("Expected a row here.");
			}
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}

	}

	/**
	 * Check that a new table, unrelated to person and address is given a different tableset ID.
	 */
	@Test
	public void checkTableSetID2(){

		TestBase.resetLocatorFile();
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try{
			sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT H2O.H2O_ONE_H2O_TM_TABLE.table_id, table_set FROM H2O.H2O_ONE_H2O_TM_TABLE, H2O.H2O_ONE_H2O_TM_REPLICA " +
			"WHERE H2O.H2O_ONE_H2O_TM_REPLICA.table_id=H2O.H2O_ONE_H2O_TM_TABLE.table_id ORDER BY table_id;");

			ResultSet rs = sa.getResultSet();

			int firstTableSet = -2;
			if (rs.next()){
				firstTableSet = rs.getInt(2);
			}
			if (rs.next()){
				assertEquals(firstTableSet, rs.getInt(2));
			} else {
				fail("Expected a row here.");
			}
			if (rs.next()){
				assertNotSame(firstTableSet, rs.getInt(2));
			}

			sa.equals("DROP TABLE TEST2");
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}

		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "END OF LAST TEST (MAIN BODY).");

	}

	//Do the same tests for replicas ebing created.

	//	/**
	//	 * Check that the replicated copy of person maintains its primary key. Tested by trying
	//	 * to break this integrity constraint.
	//	 */
	//	@Test
	//	public void checkPrimaryKeyHeld(){
	//
	//		try{
	//
	//			sb.execute("CREATE REPLICA Person, Address;");
	//
	//			if (sb.getUpdateCount() != 0){
	//				fail("Expected update count to be '0'");
	//			}
	//
	//			try{
	//				sb.execute("INSERT INTO Person VALUES (0, 'One Person Too Many', 0);");
	//
	//				fail("This should have caused an exception to be thrown.");
	//
	//			} catch(JdbcSQLException e){
	//				//Expected.
	//			}
	//
	//		} catch (SQLException e){
	//			e.printStackTrace();
	//			fail("An Unexpected SQLException was thrown.");
	//		}
	//	}
	//
	//	/**
	//	 * Try to delete from the address table - should fail because it violates referential integrity.
	//	 */
	//	@Test
	//	public void deleteFromAddress(){
	//
	//		try{
	//
	//			sb.execute("CREATE REPLICA Person, Address;");
	//
	//			if (sb.getUpdateCount() != 0){
	//				fail("Expected update count to be '0'");
	//			}
	//
	//			try{
	//				sb.execute("DELETE FROM ADDRESS;");
	//
	//				sb.execute("SELECT LOCAL ONLY * FROM ADDRESS;");
	//				
	//				ResultSet rs = sb.getResultSet();
	//				
	//				if (!rs.next()){
	//					System.err.println("Contents were deleted.");
	//				}
	//				
	//				fail("This should have caused an exception to be thrown.");
	//
	//			} catch(JdbcSQLException e){
	//				//Expected.
	//			}
	//
	//			
	//		} catch (SQLException e){
	//			e.printStackTrace();
	//			fail("An Unexpected SQLException was thrown.");
	//		}
	//	}
}
