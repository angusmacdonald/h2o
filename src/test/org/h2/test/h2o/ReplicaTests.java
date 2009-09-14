package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.h2o.comms.DatabaseURL;
import org.junit.Test;

import uk.ac.stand.dcs.nds.util.Diagnostic;

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
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
			
		}
	}

	/**
	 * Check that the schema manager is correctly updated when a new replica is created.
	 */
	@Test
	public void SchemaMetaData(){

		try{
			
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("SELECT * FROM H2O.H2O_REPLICA;");
			
			ResultSet rs = sa.getResultSet();

			if (!(rs.next() && rs.next())){
				fail("Should have been two entries in the schema manager.");
			}

		} catch (SQLException sqle){

			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
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
			sqle.printStackTrace();
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
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{
			sb.execute("CREATE REPLICA TEST");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Quite"};

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
	 * Tests that the SELECT LOCAL ONLY command fails when no local copy is available.
	 */
	@Test
	public void SelectLocalTestFailure(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{
			sb.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

			fail("It shouldn't be possible to query a local version which doesn't exist.");
		} catch (SQLException sqle){
			//Expected!
		}
	}

	/**
	 * Tests that the SELECT LOCAL command find a remote copy when ONLY is not used.
	 */
	@Test
	public void SelectLocalTestRemote(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{
			Diagnostic.traceNoEvent(Diagnostic.FULL, "Executing test query.");
			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");
		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("The table should be found remotely if not available locally.");
		}
	}

	/**
	 * Tests that the SELECT PRIMARY command succeeds, in the case where the primary is local.
	 */
	@Test
	public void SelectPrimaryWhenLocal(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;");

			int[] pKey = {1, 2};
			String[] secondCol = {"Hello", "World"};

			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			fail("This should succeed.");
		}
	}

//	/**
//	 * Tests that the SELECT PRIMARY command works - this is done by updating one copy but not the other, so that they can be told apart.
//	 */
//	@Test
//	public void SelectPrimaryTest(){
//
//		try{
//			sb.execute("CREATE REPLICA TEST");
//
//			if (sb.getUpdateCount() != 0){
//				fail("Expected update count to be '0'");
//			}
//
//			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
//
//			/*
//			 * Check that the local copy has only two entries.
//			 */
//			sb.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");
//
//			int[] pKey = {1, 2};
//			String[] secondCol = {"Hello", "World"};
//
//			validateResults(pKey, secondCol, sb.getResultSet());
//
//			/*
//			 * Check that the primary copy has three entries.
//			 */
//			sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); //Now query on first machine (which should have one extra row).
//
//			int[] pKey2 = {1, 2, 3};
//			String[] secondCol2 = {"Hello", "World", "Quite"};
//
//			validateResults(pKey2, secondCol2, sb.getResultSet());
//
//		} catch (SQLException sqle){
//			System.out.println("This trace matters:");
//			sqle.printStackTrace();
//			fail("SQLException thrown when it shouldn't have.");
//		}
//	}

	/**
	 * Tests the 'push replication' feature by attempting to initiate replication creation on database B from database A.
	 */
	@Test
	public void PushReplicationON(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		
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

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Quite"};

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
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		
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

			sc.close();
			cc.close();

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
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
			
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
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
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
	 * Checks that the DROP REPLICA command works for a non-primary replica.
	 */
	@Test
	public void DropReplica(){

		try{

			sb.execute("CREATE REPLICA TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sb.execute("INSERT INTO TEST VALUES(3, 'Quite');");

			try{
				sb.execute("DROP REPLICA TEST;");
			} catch(SQLException e){
				e.printStackTrace();
				fail("Failed to drop replica.");
			}

			try{
				sb.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

				fail("Failed to drop replica.");
			}	catch (SQLException e){
				//Expected.
			}

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Checks that its possible to drop the primary, original copy of the data when one other copy exists.
	 */
	@Test
	public void DropPrimaryReplica(){

		try{

			sb.execute("CREATE REPLICA TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			try{
				sa.execute("DROP REPLICA TEST;");
			} catch(SQLException e){
				e.printStackTrace();
				fail("Failed to drop replica.");
			}

			try{
				sa.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

				fail("Failed to drop replica.");
			}	catch (SQLException e){
				//Expected.	
			}

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Checks that an error is thrown when database B tries to drop a replica when one doesn't exist on that database.
	 */
	@Test
	public void DropReplicaFail(){

		try{
			sb.execute("DROP REPLICA TEST;");
			fail("Succeeded in dropping the replica when it shouldn't have been possible.");
		} catch(SQLException e){
			//Expected
		}

	}

	/**
	 * Checks that its possible to drop the primary, original copy of the data when no other copy exists.
	 * 
	 * <p>By default this operation should fail - DROP TABLE should be used instead.
	 */
	@Test
	public void DropOnlyReplicaFail(){

		try{
			sa.execute("DROP REPLICA TEST;");
			fail("Succeeded in dropping the replica when it shouldn't have been possible.");
		} catch(SQLException e){
			//Expected
		}

	}

	/**
	 * Tests the feature to create multiple replicas at the same time.
	 */
	@Test
	public void CreateMultipleReplicas(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST2 VALUES(4, 'Meh');");
			sa.execute("INSERT INTO TEST2 VALUES(5, 'Heh');");

			sb.execute("CREATE REPLICA TEST, TEST2;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Check that the local copy has only two entries.
			 */
			sb.execute("SELECT LOCAL ONLY * FROM TEST2 ORDER BY ID;");

			int[] pKey = {4, 5};
			String[] secondCol = {"Meh", "Heh"};

			validateResults(pKey, secondCol, sb.getResultSet());


		} catch (SQLException e){
			e.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Tests the feature to create multiple replicas at the same time using the ON and FROM syntax.
	 */
	@Test
	public void CreateMultipleReplicasONFROM(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST2 VALUES(4, 'Meh');");
			sa.execute("INSERT INTO TEST2 VALUES(5, 'Heh');");

			sa.execute("CREATE REPLICA TEST, TEST2 ON 'jdbc:h2:mem:two' FROM 'jdbc:h2:mem:one';");

			if (sa.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Check that the local copy has only two entries.
			 */
			sb.execute("SELECT LOCAL ONLY * FROM TEST2 ORDER BY ID;");

			int[] pKey = {4, 5};
			String[] secondCol = {"Meh", "Heh"};

			validateResults(pKey, secondCol, sb.getResultSet());

		} catch (SQLException e){
			e.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}
}
