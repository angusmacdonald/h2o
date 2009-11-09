package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.junit.Test;


/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultiQueryTransactionTests extends TestBase{

	/**
	 * The number of times a command should be repeated.
	 */
	private static final int TOTAL_ITERATIONS = 100;

	/**
	 * Attempts to insert a number of queries into the database in the same transaction. Update on a single replica.
	 */
	@Test
	public void basicMultiQueryInsert(){
		try{

			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);

			System.out.println("About to do the big set of inserts:");
			sa.execute(sqlToExecute);

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Attempts to insert a number of queries into the database in the same transaction.
	 * Same as @see {@link #basicMultiQueryInsert()} but this executes the query remotely with a linked table connection.
	 */
	@Test
	public void basicMultiQueryInsertRemote(){
		try{

			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);

			System.out.println("About to do the big set of inserts:");
			sb.execute(sqlToExecute);

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Attempts to insert a number of queries into the database in the same transaction, then deletes
	 * a large number of these queries (again, part of the same transaction), and checks the result.
	 */
	@Test
	public void basicMultiQueryDelete(){
		try{

			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);


			/*
			 * Delete some of these entries...
			 */
			int toDelete = TOTAL_ITERATIONS/2;

			for (int i = (toDelete/2); i < toDelete; i++){
				sqlToExecute += "DELETE FROM TEST WHERE ID = " + i + ";";
				pKey[i-1] = 0;
				secondCol[i-1] = null;
			}

			sb.execute(sqlToExecute);

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Attempts to insert a number of queries into the database in the same transaction, this time
	 * in the case where there are multiple replicas (meaning the updates must be propagated).
	 */
	@Test
	public void multiQueryPropagatedInserts(){
		try{

			/*
			 * Create replica on B.
			 */
			sb.execute("CREATE REPLICA TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);

			sa.execute(sqlToExecute); //Insert test rows.

			//Validate on first replica
			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


			//Validate on second replica
			sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sb.getResultSet());

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Tests that a local multi-query transaction will rollback if there is a failure before a commit.
	 */
	@Test
	public void testFailureLocal(){
		try{
			Constants.IS_TESTING_QUERY_FAILURE = true;

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);

			try{
				sa.execute(sqlToExecute); //Insert test rows.

				fail("This should have thrown an exception");
			} catch (SQLException e){
				//Expected.
			}

			//Re-set row contents (nothing should have been inserted by this transaction.
			pKey = new int[ROWS_IN_DATABASE];
			secondCol = new String[ROWS_IN_DATABASE];
			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			//Validate on first replica
			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Tests that a multi-query transaction involving more than one database instance will rollback if there is a failure before a commit.
	 */
	@Test
	public void testFailureRemote(){
		try{
			Constants.IS_TESTING_QUERY_FAILURE = true;

			/*
			 * Create replica on B.
			 */
			sb.execute("CREATE REPLICA TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			String sqlToExecute = createMultipleInserts(pKey, secondCol);

			try{
				sa.execute(sqlToExecute); //Insert test rows.

				fail("This should have thrown an exception");
			} catch (SQLException e){
				//Expected.
			}

			//Re-set row contents (nothing should have been inserted by this transaction.
			pKey = new int[ROWS_IN_DATABASE];
			secondCol = new String[ROWS_IN_DATABASE];
			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			//Validate on first replica
			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


			//Validate on second replica
			sb.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sb.getResultSet());

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Creates lots of insert statements for testing.
	 */
	private String createMultipleInserts(int[] pKey,
			String[] secondCol) {
		String sqlToExecute = "";

		pKey[0] = 1; pKey[1] = 2;
		secondCol[0] = "Hello"; secondCol[1] = "World";

		for (int i = (ROWS_IN_DATABASE+1); i < (TOTAL_ITERATIONS+(ROWS_IN_DATABASE+1)); i++){
			pKey[i-1] = i;
			secondCol[i-1] = "helloNumber" + i;

			sqlToExecute += "INSERT INTO TEST VALUES(" + pKey[i-1] + ", '" + secondCol[i-1] + "'); ";
		}
		return sqlToExecute;
	}
}
