package org.h2.test.h2o;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

/**
 * Various tests for replication in H2O. The 'CREATE REPLICA' function in particular.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaTests extends TestBase{



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

			sb.execute("SELECT * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};
			
			validateResults(pKey, secondCol, sb.getResultSet());


		} catch (SQLException sqle){
			fail("SQLException thrown when it shouldn't have.");
			sqle.printStackTrace();
		}
	}

	/**
	 * Tests that the SELECT LOCAL command works - this is done by updating one copy but not the other, so that they can be told apart.
	 */
	@Test
	public void SelectLocalTest(){

		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};
			
			validateResults(pKey, secondCol, sb.getResultSet());

			sa.execute("SELECT LOCAL * FROM TEST ORDER BY ID;"); //Now query on first machine (which should have one extra row).

			int[] pKey2 = {1, 2, 3};
			String[] secondCol2 = {"Hello", "World", "Quite"};
			
			validateResults(pKey2, secondCol2, sa.getResultSet());
			
		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
			}
	}

	/**
	 * Tests that the SELECT PRIMARY command works - this is done by updating one copy but not the other, so that they can be told apart.
	 */
	@Test
	public void SelectPrimaryTest(){

		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
			
			/*
			 * Check that the local copy has only two entries.
			 */
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};
			
			validateResults(pKey, secondCol, sb.getResultSet());

			/*
			 * Check that the primary copy has three entries.
			 */
			sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); //Now query on first machine (which should have one extra row).

			int[] pKey2 = {1, 2, 3};
			String[] secondCol2 = {"Hello", "World", "Quite"};
			
			validateResults(pKey2, secondCol2, sb.getResultSet());
			
		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
			}
	}
	
	/**
	 * Tests the 'push replication' feature by attempting to initiate replication creation on database B from database A.
	 */
	@Test
	public void PushReplication(){

		try{
			sa.execute("CREATE REPLICA TEST ON 'jdbc:h2:mem:two'");

			if (sa.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
			
			/*
			 * Check that the local copy has only two entries.
			 */
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};
			
			validateResults(pKey, secondCol, sb.getResultSet());

			/*
			 * Check that the primary copy has three entries.
			 */
			sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); //Now query on first machine (which should have one extra row).

			int[] pKey2 = {1, 2, 3};
			String[] secondCol2 = {"Hello", "World", "Quite"};
			
			validateResults(pKey2, secondCol2, sb.getResultSet());
			
		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
			}
	}
	/**
	 * Utility method which checks that the results of a test query match up to the set of expected values. The 'TEST'
	 * class is being used in these tests so the primary keys (int) and names (varchar/string) are required to check the
	 * validity of the resultset.
	 * @param key			The set of expected primary keys.
	 * @param secondCol		The set of expected names.
	 * @param rs			The results actually returned.
	 * @throws SQLException 
	 */
	private void validateResults(int[] pKey, String[] secondCol, ResultSet rs) throws SQLException {
		for (int i=0; i < pKey.length; i++){
			if (rs.next()){
				assertEquals(pKey[i], rs.getInt(1));
				assertEquals(secondCol[i], rs.getString(2));
			} else {
				fail("Expected an entry here.");
			}
		}

		if (rs.next()){
			fail("Too many entries.");
		}
		
		rs.close();
	}
}
