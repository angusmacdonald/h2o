package org.h2.test.h2o;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.SchemaManager;
import org.h2.jdbc.JdbcSQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.stand.dcs.nds.util.Diagnostic;

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

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		SchemaManager.USERNAME = "sa";
		SchemaManager.PASSWORD = "sa";

		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", "sa", "sa");
		cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");

		sa = ca.createStatement();
		sb = cb.createStatement();

		sa.execute("CREATE TABLE Address(id INT NOT NULL, street VARCHAR(255), PRIMARY KEY (id));");

		sa.execute("CREATE TABLE Person(id INT NOT NULL, name VARCHAR(255), address_id INT NOT NULL, PRIMARY KEY (id), FOREIGN KEY (address_id) REFERENCES Address (id));");

		sa.execute("INSERT INTO Address VALUES (0, 'Pinewood Avenue');");
		sa.execute("INSERT INTO Address VALUES (1, 'Kinnessburn Terrace');");
		sa.execute("INSERT INTO Address VALUES (2, 'Lamond Drive');");
		sa.execute("INSERT INTO Address VALUES (3, 'North Street');");
		sa.execute("INSERT INTO Address VALUES (4, 'Market Street');");
		sa.execute("INSERT INTO Address VALUES (5, 'Hawthorn Avenue');");

		sa.execute("INSERT INTO Person VALUES (0, 'Angus Macdonald', 0);");
		sa.execute("INSERT INTO Person VALUES (1, 'Alan Dearle', 1);");
		sa.execute("INSERT INTO Person VALUES (2, 'Graham Kirby', 2);");
		sa.execute("INSERT INTO Person VALUES (3, 'Dharini Balasubramaniam', 2);");
		sa.execute("INSERT INTO Person VALUES (4, 'Jon Lewis', 3);");

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
			Diagnostic.traceNoEvent(Diagnostic.FULL, "END OF LAST TEST (TEAR DOWN 1).");
			TestBase.closeDatabaseCompletely();
		} catch (Exception e){
			e.printStackTrace();
			fail("Connections aren't bein closed correctly.");
		}
		
		Diagnostic.traceNoEvent(Diagnostic.FULL, "END OF LAST TEST (TEAR DOWN 2).");
	}

	/**
	 * Check that a tableset ID is correctly given for all new tables, meaning that tables which refer to one another
	 * are given the same ID.
	 */
	@Test
	public void checkTableSetID(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{
		
					sa.execute("SELECT H2O.H2O_TABLE.table_id, table_set FROM H2O.H2O_TABLE, H2O.H2O_REPLICA WHERE H2O.H2O_REPLICA.table_id=H2O.H2O_TABLE.table_id ORDER BY table_id;");
		
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
				} catch (SQLException sqle){
					sqle.printStackTrace();
					fail("SQLException thrown when it shouldn't have.");
				}
				
	}
	
	/**
	 * Check that a new table, unrelated to person and address is given a different tableset ID.
	 */
	@Test
	public void checkTableSetID2(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{
					sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			
					sa.execute("SELECT H2O.H2O_TABLE.table_id, table_set FROM H2O.H2O_TABLE, H2O.H2O_REPLICA WHERE H2O.H2O_REPLICA.table_id=H2O.H2O_TABLE.table_id ORDER BY table_id;");
		
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
				} catch (SQLException sqle){
					sqle.printStackTrace();
					fail("SQLException thrown when it shouldn't have.");
				}
				
				Diagnostic.traceNoEvent(Diagnostic.FULL, "END OF LAST TEST (MAIN BODY).");

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
//		} catch (SQLException sqle){
//			sqle.printStackTrace();
//			fail("SQLException thrown when it shouldn't have.");
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
//		} catch (SQLException sqle){
//			sqle.printStackTrace();
//			fail("SQLException thrown when it shouldn't have.");
//		}
//	}
}
