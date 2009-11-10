package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.ResultSet;
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

			TestQuery queryToExecute = createInsertsForTestTable();

			System.out.println("About to do the big set of inserts:");
			sa.execute(queryToExecute.getSQL());

			validateOnFirstMachine(queryToExecute);


		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Attempts to insert a number of queries into the database in the same transaction.
	 * Same as @see {@link #basicMultiQueryInsert()} but this executes the query remotely with a linked table connection.
	 */
	@Test
	public void basicMultiQueryInsertRemote(){
		try{

			TestQuery queryToExecute = createInsertsForTestTable();

			System.out.println("About to do the big set of inserts:");
			sb.execute(queryToExecute.getSQL());

			validateOnFirstMachine(queryToExecute);


		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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

			validateOnFirstMachine(queryToExecute.getTableName(), pKey, secondCol);


		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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
			TestQuery queryToExecute = createInsertsForTestTable();

			sa.execute(queryToExecute.getSQL()); //Insert test rows.

			validateOnFirstMachine(queryToExecute);
			validateOnSecondMachine(queryToExecute);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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

			validateOnFirstMachine(queryToExecute.getTableName(), pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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

			validateOnFirstMachine(queryToExecute.getTableName(), pKey, secondCol);

			validateOnSecondMachine(queryToExecute.getTableName(), pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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

			validateOnFirstMachine(testQuery);



			validateOnFirstMachine("TEST2", pKey, secondCol);
			//validateOnSecondReplica(queryToExecute);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that a multi-query transaction involving more than one table works when involving multiple machines.
	 */
	@Test
	public void testMultiTableTransactionSuccessRemote(){
		try{
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

			validateOnFirstMachine(testQuery);

			validateOnSecondMachine("TEST2", pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that a multi-query transaction involving:
	 * <ul><li><Multiple tables</li>
	 * <li>Multiple replicas for a single table</li>
	 * </ul>
	 * <p>Where the outcome should be a successful query execution.
	 */
	@Test
	public void multiTableAndReplicaSuccess(){
		try{
			//Constants.IS_TESTING_QUERY_FAILURE = true;

			createSecondTable(sb, "TEST2");
			createReplicaOnB();

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

			validateOnFirstMachine(testQuery);
			validateOnSecondMachine(testQuery);

			validateOnSecondMachine("TEST2", pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that a multi-query transaction involving:
	 * <ul><li><Multiple tables</li>
	 * <li>Multiple replicas for a single table</li>
	 * </ul>
	 * <p>Where the outcome should be a FAILED QUERY EXECUTION.
	 */
	@Test
	public void multiTableAndReplicaFailure(){
		try{

			createSecondTable(sb, "TEST2");
			createReplicaOnB();

			/*
			 * Create then execute INSERTS for TEST table.
			 */

			TestQuery testQuery = createInsertsForTestTable();


			TestQuery test2query = createMultipleInsertStatements("TEST2", new int[TOTAL_ITERATIONS + ROWS_IN_DATABASE], new String[TOTAL_ITERATIONS + ROWS_IN_DATABASE], 6);


			/*
			 * Test that the state of the database is as expected, before attempting to insert anything else.
			 */
			int[] pKey = new int[ROWS_IN_DATABASE];
			String[] secondCol = new String[ROWS_IN_DATABASE];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";
			validateOnFirstMachine("TEST", pKey, secondCol);

			Constants.IS_TESTING_QUERY_FAILURE = true;
			try{
				sa.execute(testQuery.getSQL() + test2query.getSQL()); //Insert test rows.
				fail("This query should have failed.");
			} catch(SQLException e){
				//Expected
			}


			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";


			validateOnFirstMachine("TEST", pKey, secondCol);

			//validateOnSecondMachine(testQuery);


			pKey[0] = 4; pKey[1] = 5;
			secondCol[0] = "Meh"; secondCol[1] = "Heh";

			validateOnSecondMachine("TEST2", pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that when a transaction fails to create a table the schema manager
	 * is not updated with information on that table.
	 * 
	 * <p>TESTS AFTER A CREATE TABLE STATEMENT HAS BEEN RUN, BUT BEFORE ANYTHING ELSE.
	 */
	@Test
	public void testSchemaManagerContents(){
		try{

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next() && rs.next()){
				fail("There should only be one table in the schema manager.");
			}

			Constants.IS_TESTING_CREATETABLE_FAILURE = true;
			Constants.IS_TESTING_QUERY_FAILURE = true;

			try{
				createSecondTable(sb, "TEST2");
				fail("This should have failed.");
			} catch (SQLException e){
				//Expected.
			}

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");

			rs = sa.getResultSet();

			if (rs.next() && rs.next()){
				fail("There should only be one table in the schema manager.");
			}
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that when a transaction fails to create a table the schema manager
	 * is not updated with information on that table.
	 * 
	 * <p>TESTS AFTER A CREATE TABLE STATEMENT HAS BEEN RUN, AND AFTER SOME INSERTS
	 * INTO THAT TABLE.
	 */
	@Test
	public void testSchemaManagerContentsAfterInsert(){
		try{

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next() && rs.next()){
				fail("There should only be one table in the schema manager.");
			}

			Constants.IS_TESTING_QUERY_FAILURE = true;

			try{
				createSecondTable(sb, "TEST2");
				fail("This should have failed.");
			} catch (SQLException e){
				//Expected.
			}

			try {
				sa.execute("SELECT * FROM TEST2");
				fail("This should have failed: the transaction was not committed.");
			} catch (SQLException e){
				//Expected.
			}


			sa.execute("SELECT * FROM H2O.H2O_TABLE;");

			rs = sa.getResultSet();

			if (rs.next() && rs.next()){
				fail("There should only be one table in the schema manager.");
			}
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
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

			sqlToExecute += "INSERT INTO " + tableName + " VALUES(" + pKey[i-1] + ", '" + secondCol[i-1] + "');\n";
		}
		return new TestQuery(sqlToExecute, tableName, pKey, secondCol);
	}
}
