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
package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.LocatorServer;
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
	 * Tests that when a transaction fails to create a table the System Table
	 * is not updated with information on that table.
	 * 
	 * <p>TESTS AFTER A CREATE TABLE STATEMENT HAS BEEN RUN, BUT BEFORE ANYTHING ELSE.
	 */
	@Test
	public void testSystemTableContents(){
		try{

			sa.execute("SELECT * FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next() && rs.next()){
				fail("There should only be one table in the System Table.");
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
				fail("There should only be one table in the System Table.");
			}
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	//	/**
	//	 * Tests that when a transaction fails to create a table the System Table
	//	 * is not updated with information on that table.
	//	 * 
	//	 * <p>TESTS AFTER A CREATE TABLE STATEMENT HAS BEEN RUN, AND AFTER SOME INSERTS
	//	 * INTO THAT TABLE.
	//	 */
	//	@Test
	//	public void testSystemTableContentsAfterInsert(){
	//		try{
	//
	//			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
	//
	//			ResultSet rs = sa.getResultSet();
	//
	//			if (rs.next() && rs.next()){
	//				fail("There should only be one table in the System Table.");
	//			}
	//
	//			Constants.IS_TESTING_QUERY_FAILURE = true;
	//
	//			try{
	//				createSecondTable(sb, "TEST2");
	//				fail("This should have failed.");
	//			} catch (SQLException e){
	//				//Expected.
	//			}
	//
	//			try {
	//				sa.execute("SELECT * FROM TEST2");
	//				fail("This should have failed: the transaction was not committed.");
	//			} catch (SQLException e){
	//				//Expected.
	//			}
	//
	//
	//			sa.execute("SELECT * FROM H2O.H2O_TABLE;");
	//
	//			rs = sa.getResultSet();
	//
	//			if (rs.next() && rs.next()){
	//				fail("There should only be one table in the System Table.");
	//			}
	//		} catch (SQLException e){
	//			e.printStackTrace();
	//			fail("An Unexpected SQLException was thrown.");
	//		}
	//	}

	/**
	 * Test executing a set of queries where the external application explicitly turns auto-commit off.
	 */
	@Test
	public void testAutoCommitOffExternal(){

		try {
			ca.setAutoCommit(false);

			//Execute some queries

			sa.execute("INSERT INTO TEST VALUES(3, 'Quite');");
			sa.execute("INSERT INTO TEST VALUES(4, 'A');");
			sa.execute("INSERT INTO TEST VALUES(5, 'Few');");
			sa.execute("INSERT INTO TEST VALUES(6, 'Cases');");

			ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
			cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

			Statement sa2 = cb.createStatement();

			try{
				ResultSet rs = sa2.executeQuery("SELECT LOCAL * FROM TEST ORDER BY ID;");

				fail("Query timeout expected.");

			}catch(SQLException e){
				//Timeout expected.
			}


			//Commit
			ca.commit();

			//Check that changes have now been committed.

			sa.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			int[] pKey2 = {1, 2, 3, 4, 5, 6};
			String[] secondCol2 = {"Hello", "World", "Quite", "A", "Few", "Cases"};

			validateResults(pKey2, secondCol2, sa.getResultSet());

			sa.execute("SELECT LOCAL * FROM TEST ORDER BY ID;");

			ca.setAutoCommit(true);

		} catch (SQLException e) {
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}


	/**
	 * Tests that prepared statements work in the system where no replication is involved.
	 */
	@Test
	public void testPreparedStatementsNoReplication(){

		PreparedStatement mStmt = null;
		try
		{
			mStmt = ca.prepareStatement( "insert into PUBLIC.TEST (id,name) values (?,?)" );


			for (int i = 3; i < 100; i++){
				mStmt.setInt(1, i);
				mStmt.setString(2, "helloNumber" + i);
				mStmt.addBatch();
			}

			mStmt.executeBatch();

			int[] pKey = new int[100];
			String[] secondCol = new String[100];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			TestQuery test2query = createMultipleInsertStatements("TEST", pKey, secondCol, 3);

			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());

		} catch ( SQLException ex ) {
			ex.printStackTrace();
			fail("Unexpected SQL Exception was thrown. Not cool.");
		} 
	}

	/**
	 * Tests that prepared statements work in the system where no replication is involved.
	 * @throws SQLException 
	 */
	@Test
	public void testPreparedStatementsUpdateNoReplication() throws SQLException{
		//update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
		createReplicaOnB();

		PreparedStatement mStmt = null;
		try
		{
			mStmt = ca.prepareStatement( "insert into PUBLIC.TEST (id,name) values (?,?)" );


			for (int i = 3; i < 10; i++){
				mStmt.setInt(1, i);
				mStmt.setString(2, "helloNumber" + i);
				mStmt.addBatch();
			}

			mStmt.executeBatch();

			mStmt = ca.prepareStatement( "update PUBLIC.TEST set name=? where id=?;" );
			mStmt.setString(1, "New Order");
			mStmt.setInt(2, 1);
			mStmt.addBatch();
			mStmt.executeBatch();


			int[] pKey = new int[10];
			String[] secondCol = new String[10];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "New Order"; secondCol[1] = "World";

			TestQuery test2query = createMultipleInsertStatements("TEST", pKey, secondCol, 3);

			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());

		} catch ( SQLException ex ) {
			ex.printStackTrace();
			fail("Unexpected SQL Exception was thrown. Not cool.");
		} 
	}

	/**
	 * Tests that prepared statements work in the system where no replication is involved.
	 * @throws SQLException 
	 */
	@Test
	public void testPreparedStatementsDeleteNoReplication() throws SQLException{
		//update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
		createReplicaOnB();



		PreparedStatement mStmt = null;
		try
		{
			mStmt = ca.prepareStatement( "insert into PUBLIC.TEST (id,name) values (?,?)" );


			for (int i = 3; i < 10; i++){
				mStmt.setInt(1, i);
				mStmt.setString(2, "helloNumber" + i);
				mStmt.addBatch();
			}

			mStmt.executeBatch();


			mStmt = ca.prepareStatement( "delete from PUBLIC.TEST where id=?;" );
			mStmt.setInt(1, 9);
			mStmt.addBatch();
			mStmt.executeBatch();
			//				

			int[] pKey = new int[9];
			String[] secondCol = new String[9];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			TestQuery test2query = createMultipleInsertStatements("TEST", pKey, secondCol, 3);

			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());

		} catch ( SQLException ex ) {
			ex.printStackTrace();
			fail("Unexpected SQL Exception was thrown. Not cool.");
		} 
	}
	//	/**
	//	 * Tests that prepared statements work in the system where no replication is involved.
	//	 */
	//	@Test
	//	public void testPreparedStatementsMultipleTransactions(){
	//
	//		PreparedStatement mStmt = null;
	//		try
	//		{
	//			Connection cc = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
	//			
	//			cc.setAutoCommit(false);
	//			mStmt = cc.prepareStatement( "CREATE TABLE PUBLIC.TEST5 (ID INT, NAME VARCHAR(255));" );
	//
	//
	//			mStmt.execute();
	//
	//			cc.commit();
	//			
	//			cc.close();
	//			
	//			while (!cc.isClosed()){};
	//			
	//		} catch ( SQLException ex ) {
	//			ex.printStackTrace();
	//			fail("Unexpected SQL Exception was thrown. Not cool.");
	//		} 
	//		
	//		
	//		mStmt = null;
	//		try
	//		{
	//
	//			
	//			mStmt = ca.prepareStatement( "insert into PUBLIC.TEST5 (id,name) values (?,?)" );
	//
	//
	//			for (int i = 3; i < 100; i++){
	//				mStmt.setInt(1, i);
	//				mStmt.setString(2, "helloNumber" + i);
	//				mStmt.addBatch();
	//			}
	//
	//			mStmt.executeBatch();
	//
	//			int[] pKey = new int[100];
	//			String[] secondCol = new String[100];
	//
	//			pKey[0] = 1; pKey[1] = 2;
	//			secondCol[0] = "Hello"; secondCol[1] = "World";
	//
	//			TestQuery test2query = createMultipleInsertStatements("TEST5", pKey, secondCol, 3);
	//
	//			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST5 ORDER BY ID;");
	//
	//			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
	//
	//		} catch ( SQLException ex ) {
	//			ex.printStackTrace();
	//			fail("Unexpected SQL Exception was thrown. Not cool.");
	//		} 
	//	}

	/**
	 * Tests that prepared statements work in the system where replication is involved.
	 */
	@Test
	public void testPreparedStatementsReplication(){


		PreparedStatement mStmt = null;
		try
		{
			createReplicaOnB();


			mStmt = cb.prepareStatement( "insert into PUBLIC.TEST (id,name) values (?,?)" );


			for (int i = 3; i < 100; i++){
				mStmt.setInt(1, i);
				mStmt.setString(2, "helloNumber" + i);
				mStmt.addBatch();
			}

			mStmt.executeBatch();

			int[] pKey = new int[100];
			String[] secondCol = new String[100];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			TestQuery test2query = createMultipleInsertStatements("TEST", pKey, secondCol, 3);

			/*
			 * If the query hangs and fails at this point its probably because the lock isn't being relinquished when executeBatch is called.
			 */
			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());

		} catch ( SQLException ex ) {
			ex.printStackTrace();
			fail("Unexpected SQL Exception was thrown. Not cool.");
		}
	}

	/**
	 * Tests that prepared statements work in the system where no replication is involved, and the connection
	 * is made through a TCP server. This test uses some of the *Remote classes in H2 which have slightly different
	 * behaviour.
	 */
	@Test
	public void testPreparedStatementsTcpServer(){

		/*
		 * Reset the locator file. This test doesn't use the in-memory database.
		 */
		ls.setRunning(false);
		while (!ls.isFinished()){};

		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		try {
			DeleteDbFiles.execute("db_data/unittests/", "schema_test", true);
		} catch (SQLException e) { }
		Connection conn = null;
		// start the server, allows to access the database remotely
		Server server = null;
		try {
			server = Server.createTcpServer(new String[] { "-tcpPort", "9990", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test" });
			server.start();

			Class.forName("org.h2.Driver");
			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

			Statement sa = conn.createStatement();

			sa.execute("DROP ALL OBJECTS;");
			sa.execute("CREATE TABLE TEST5(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST5 VALUES(1, 'Hello');");
			sa.execute("INSERT INTO TEST5 VALUES(2, 'World');");

			server.shutdown();
			server.stop();

			TestBase.resetLocatorFile();

			server = Server.createTcpServer(new String[] { "-tcpPort", "9990", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test" });

			server.start();

			conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);


			PreparedStatement mStmt = conn.prepareStatement( "insert into PUBLIC.TEST5 (id,name) values (?,?)" );


			for (int i = 3; i < 100; i++){
				mStmt.setInt(1, i);
				mStmt.setString(2, "helloNumber" + i);
				mStmt.addBatch();
			}

			mStmt.executeBatch();

			int[] pKey = new int[100];
			String[] secondCol = new String[100];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";

			TestQuery test2query = createMultipleInsertStatements("TEST5", pKey, secondCol, 3);

			sa = conn.createStatement();

			sa.execute("SELECT LOCAL * FROM PUBLIC.TEST5 ORDER BY ID;");


			validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());


		} catch (SQLException e1) {
			e1.printStackTrace();
			fail("Unexpected SQL Exception was thrown. Not cool.");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn != null && !conn.isClosed()) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// stop the server
			server.stop();

			try {
				DeleteDbFiles.execute("db_data/unittests/", "schema_test", true);
			} catch (SQLException e) {}
		}

	}

	/*
	 * ###############
	 * Utility Methods
	 * ###############
	 */

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
