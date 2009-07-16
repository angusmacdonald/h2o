package org.h2.test.h2o;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");

		}
	}

	/**
	 * Tests that the contents of a table are successfully copied over.
	 */
	@Test
	public void TableData(){

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
	 * Tests that an error is returned whena replica already exists at the given location.
	 */
	@Test
	public void ReplicaAlreadyExists(){

		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}
		} catch (SQLException sqle){
			fail("This shouldn't have caused any errors.");
		}
	
		try{
			sb.execute("CREATE REPLICA TEST");

			fail("Expected an error to be thrown here, as the replica already exists..");
		} catch (SQLException sqle){
			//Expected.
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
	 * Tests that the SELECT LOCAL command fails when no local copy is available.
	 */
	@Test
	public void SelectLocalTestFailure(){

		try{

			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			fail("It shouldn't be possible to query a local version which doesn't exist.");
		} catch (SQLException sqle){
			//Expected!
		}
	}

	/**
	 * Tests that the SELECT PRIMARY command succeeds, in the case where the primary is local.
	 */
	@Test
	public void SelectPrimaryWhenLocal(){

		try{

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};

			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			fail("This should succeed.");
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
	public void PushReplicationON(){

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
	 * Tests the 'push replication' feature by attempting to initiate replication creation on database C from database A, getting the data from database B.
	 * The test first creates a replica on database B, then launches the ON-FROM replication command from database A.
	 */
	@Test
	public void PushReplicationONFROM(){

		try{
			Connection cc = DriverManager.getConnection("jdbc:h2:mem:three", "sa", "sa");
			Statement sc = cc.createStatement();

			sb.execute("CREATE REPLICA TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("CREATE REPLICA TEST ON 'jdbc:h2:mem:three' FROM 'jdbc:h2:mem:two'");

			if (sa.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sc.execute("INSERT INTO TEST VALUES(3, 'Quite');");

			/*
			 * Check that the local copy has three entries.
			 */
			sc.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Quite"};

			validateResults(pKey, secondCol, sc.getResultSet());



		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Tests the 'push replication' feature by attempting to initiate replication creation on database B from database A, using the FROM
	 * syntax, even though it is not needed. This checks that the ON, FROM syntax works when describing the machine local machine.
	 */
	@Test
	public void PushReplicationFROMtwoMachines(){

		try{

			sa.execute("CREATE REPLICA TEST ON 'jdbc:h2:mem:two' FROM 'jdbc:h2:mem:one'");

			if (sa.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sb.execute("INSERT INTO TEST VALUES(3, 'Quite');");

			/*
			 * Check that the local copy has three entries.
			 */
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Quite"};

			validateResults(pKey, secondCol, sb.getResultSet());



		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Tests the 'push replication' feature by attempting to initiate replication creation on database B from database A, using the ON
	 * syntax, even though it is not needed. This checks that the ON, FROM syntax works when describing the machine local machine.
	 */
	@Test
	public void PushReplicationFROMtwoMachinesAlt(){

		try{

			sb.execute("CREATE REPLICA TEST ON 'jdbc:h2:mem:two' FROM 'jdbc:h2:mem:one'");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sb.execute("INSERT INTO TEST VALUES(3, 'Quite');");

			/*
			 * Check that the local copy has three entries.
			 */
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Quite"};

			validateResults(pKey, secondCol, sb.getResultSet());



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
