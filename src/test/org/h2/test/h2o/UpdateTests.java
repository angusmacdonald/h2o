package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.junit.Test;


/**
 * Tests that check the ability of the system to propagate updates, both in cases where the
 * update can be committed, and in cases where it must be aborted.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class UpdateTests extends TestBase {

	/**
	 * Test that an insert is rolled back correctly when one of the database instances fails.
	 */
	@Test
	public void testRollbackCapability(){
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		Constants.IS_TESTING_PRE_PREPARE_FAILURE = true;
		
		try {
			sa.execute("INSERT INTO TEST VALUES(3, 'HAHAAHAHAHA');");

			fail("Expected failure.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Tests that the database throws an exception when an instance fails on the commit operation.
	 */
	@Test
	public void failureOnCommit(){
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch (SQLException e1) {
			fail("This wasn't even the interesting part of the test.");
		}
		
		Constants.IS_TESTING_PRE_COMMIT_FAILURE = true;
		
		try {
			sa.execute("INSERT INTO TEST VALUES(3, 'HAHAAHAHAHA');");

			fail("Expected failure.");
		} catch (SQLException e) {
			//e.printStackTrace();
		}
	
		
		try {
			/*
			 * Start up the failed connection again so that the generic tearDown stuff works.
			 */
			cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Tests that the DELETE FROM command synchronously updates all replicas.
	 */
	@Test
	public void deleteFromCommand(){
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch (SQLException e1) {
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			sa.execute("DELETE FROM TEST WHERE ID=1;");

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected failure executing DELETE FROM.");
		}
	
		
		/*
		 * Now check that each replica sees the result of the deletion.
		 */
		
		int[] pKey = {2};
		String[] secondCol = {"World"};

		try {
			sa.execute("SELECT * FROM TEST ORDER BY ID");
			validateResults(pKey, secondCol, sa.getResultSet());
			
			sb.execute("SELECT * FROM TEST ORDER BY ID");
			validateResults(pKey, secondCol, sb.getResultSet());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Validation of results failed.");
		}
	}
	
	/**
	 * Tests that the UPDATE command synchronously updates all replicas.
	 */
	@Test
	public void updateCommand(){
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch (SQLException e1) {
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			sa.execute("UPDATE TEST SET ID=3, NAME='TESTING' WHERE ID=2;");

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected failure executing DELETE FROM.");
		}
	
		
		/*
		 * Now check that each replica sees the result of the deletion.
		 */
		
		int[] pKey = {1, 3};
		String[] secondCol = {"Hello", "TESTING"};

		try {
			sa.execute("SELECT * FROM TEST ORDER BY ID");
			validateResults(pKey, secondCol, sa.getResultSet());
			
			sb.execute("SELECT * FROM TEST ORDER BY ID");
			validateResults(pKey, secondCol, sb.getResultSet());
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Validation of results failed.");
		}


	}
}
