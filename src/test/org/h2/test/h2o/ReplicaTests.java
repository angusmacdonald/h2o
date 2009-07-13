package org.h2.test.h2o;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.engine.SchemaManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Various tests for replication in H2O. The 'CREATE REPLICA' function in particular.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaTests {
	
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
		
		sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
		sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
		sa.execute("INSERT INTO TEST VALUES(2, 'World');");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		sa.execute("DROP TABLE IF EXISTS TEST");
		sb.execute("DROP TABLE IF EXISTS TEST");
		
		sa.close();
		sb.close();
		
		ca.close();	
		cb.close();	
			
	}

	/**
	 * Test to ensure that the SCRIPT TABLE command is working. This is used to get the contents of the table being replicated.
	 */
	@Test
	public void ScriptTest(){

		try{
			sa.execute("SCRIPT TABLE TEST;");

			ResultSet rs = sa.getResultSet();
			
			if (!(rs.next() && rs.next())){
				fail("Incorrect number of results returned.");
			}
			
		} catch (SQLException sqle){
			fail("SQLException thrown when it shouldn't have.");
			sqle.printStackTrace();
		}
	}
	
	/**
	 * Tries to create a replica of the test table. Tests that a new table of the same name is successfully created on the other machine.
	 */
	@Test
	public void BasicTableCopy(){

		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}
			
			sb.execute("SELECT * FROM TEST");
			
		} catch (SQLException sqle){
			fail("SQLException thrown when it shouldn't have.");
			sqle.printStackTrace();
		}
	}
	
	/**
	 * Tests that the contents of a table are successfully copied over.
	 */
	@Test
	public void TableDataTest(){

		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}
			
			sb.execute("SELECT * FROM TEST;");
			
			ResultSet rs = sb.getResultSet();
			
			if (rs.next()){
				assertEquals(2, rs.getInt(1));
				assertEquals("World", rs.getString(2));
			} else {
				fail("Expected an entry here.");
			}
			if (rs.next()){
				assertEquals(1, rs.getInt(1));
				assertEquals("Hello", rs.getString(2));
			} else {
				fail("Expected an entry here.");
			}
			
		} catch (SQLException sqle){
			fail("SQLException thrown when it shouldn't have.");
			sqle.printStackTrace();
		}
	}
}
