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

			TestQuery queryToExecute = createInsertsForTestTable();

			System.out.println("About to do the big set of inserts:");
			sa.execute(queryToExecute.getSQL());

			validateOnFirstReplica(queryToExecute);


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

			TestQuery queryToExecute = createInsertsForTestTable();

			System.out.println("About to do the big set of inserts:");
			sb.execute(queryToExecute.getSQL());

			validateOnFirstReplica(queryToExecute);


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


			TestQuery queryToExecute = createInsertsForTestTable();

			int[] pKey = queryToExecute.getPrimaryKey();
			String[] secondCol = queryToExecute.getSecondColumn();

			
			String sqlToExecute = queryToExecute.getSQL();
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

			validateOnFirstReplica(queryToExecute.getTableName(), pKey, secondCol);


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

			createReplicaOnB();

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			TestQuery queryToExecute = createInsertsForTestTable();

			sa.execute(queryToExecute.getSQL()); //Insert test rows.

			validateOnFirstReplica(queryToExecute);
			validateOnSecondReplica(queryToExecute);

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
			
			TestQuery queryToExecute = createInsertsForTestTable();

			try{
				sa.execute(queryToExecute.getSQL()); //Insert test rows.

				fail("This should have thrown an exception");
			} catch (SQLException e){
				//Expected.
			}

			//Re-set row contents (nothing should have been inserted by this transaction.
			
			int[] pKey = new int[ROWS_IN_DATABASE];
			String[] secondCol = new String[ROWS_IN_DATABASE];
			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			validateOnFirstReplica(queryToExecute.getTableName(), pKey, secondCol);

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

			createReplicaOnB();

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			TestQuery queryToExecute = createInsertsForTestTable();

			try{
				sa.execute(queryToExecute.getSQL()); //Insert test rows.

				fail("This should have thrown an exception");
			} catch (SQLException e){
				//Expected.
			}

			//Re-set row contents (nothing should have been inserted by this transaction.
			int[] pKey = new int[ROWS_IN_DATABASE];
			String[] secondCol = new String[ROWS_IN_DATABASE];
			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			validateOnFirstReplica(queryToExecute.getTableName(), pKey, secondCol);

			validateOnSecondReplica(queryToExecute.getTableName(), pKey, secondCol);

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}


	/**
	 * Tests that a multi-query transaction involving more than one table. The result should be a lot of
	 * successful inserts into each table. Only involves one database instance.
	 */
	@Test
	public void testMultiTableTransactionSuccessLocal(){
		try{
			//Constants.IS_TESTING_QUERY_FAILURE = true;

			//			createReplicaOnB();

			createSecondTable(sa, "TEST2");

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			
			TestQuery testQuery = createInsertsForTestTable();
			
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			pKey[0] = 4; pKey[1] = 5;
			secondCol[0] = "Meh"; secondCol[1] = "Heh";

			TestQuery test2query = createMultipleInsertStatements("TEST2", pKey, secondCol, 6);

			sa.execute(testQuery.getSQL() + test2query.getSQL()); //Insert test rows.

			validateOnFirstReplica(testQuery);

			

			validateOnFirstReplica("TEST2", pKey, secondCol);
			//validateOnSecondReplica(queryToExecute);

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}
	
	/**
	 * Tests that a multi-query transaction involving more than one table works when involving multiple machines.
	 */
	@Test
	public void testMultiTableTransactionSuccessRemote(){
		try{
			//Constants.IS_TESTING_QUERY_FAILURE = true;

			//			createReplicaOnB();

			createSecondTable(sb, "TEST2");

			/*
			 * Create then execute INSERTS for TEST table.
			 */
			
			TestQuery testQuery = createInsertsForTestTable();
			
			int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
			String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

			pKey[0] = 4; pKey[1] = 5;
			secondCol[0] = "Meh"; secondCol[1] = "Heh";

			TestQuery test2query = createMultipleInsertStatements("TEST2", pKey, secondCol, 6);

			sa.execute(testQuery.getSQL() + test2query.getSQL()); //Insert test rows.
			
			validateOnFirstReplica(testQuery);

			

			validateOnSecondReplica("TEST2", pKey, secondCol);
			//validateOnSecondReplica(queryToExecute);

		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}

	/**
	 * Creates lots of insert statements for testing.
	 */
	private TestQuery createInsertsForTestTable() {
		int[] pKey = new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE];
		String[] secondCol = new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE];

		pKey[0] = 1; pKey[1] = 2;
		secondCol[0] = "Hello"; secondCol[1] = "World";

		return createMultipleInsertStatements("TEST", pKey, secondCol, ROWS_IN_DATABASE + 1);
	}

	/**
	 * Creates lots of insert statements for testing with the stated table.
	 * @param tableName		Name of the table where values are being inserted.
	 * @param pKey			Array of primary key values in the table. This is a parameter in case the calling method
	 * 	wants to add some custom values initially.
	 * @param secondCol		Array of values for the second column in the table. This is a parameter in case the calling method
	 * 	wants to add some custom values initially.
	 * @return The query to be executed and the expected results from this execution.
	 */
	private TestQuery createMultipleInsertStatements(String tableName, int[] pKey,
			String[] secondCol, int startPoint) {
		String sqlToExecute = "";

		for (int i = startPoint; i < (pKey.length); i++){
			pKey[i-1] = i;
			secondCol[i-1] = "helloNumber" + i;

			sqlToExecute += "INSERT INTO " + tableName + " VALUES(" + pKey[i-1] + ", '" + secondCol[i-1] + "'); ";
		}
		return new TestQuery(sqlToExecute, tableName, pKey, secondCol);
	}
}
