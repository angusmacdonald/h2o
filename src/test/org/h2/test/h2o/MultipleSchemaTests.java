package org.h2.test.h2o;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * A small set of tests that check whether the system copes with
 * multiple different schemas (containing tables of the same name).
 * 
 * <p>Tests include basic queries and testing the ability to replicate schema's.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultipleSchemaTests extends TestBase {

	/**
	 * Tests that a table in a non-default schema is added succussfully to the schema manager.
	 */
	@Test
	public void TestSchemaManagerAdd(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{

			sa.execute("CREATE SCHEMA SCHEMA2");
			sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("PUBLIC", rs.getString(2));
			} 
			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("SCHEMA2", rs.getString(2));
			} else {
				fail("Expected a schema manager entry here.");
			}

			sa.execute("DROP TABLE IF EXISTS SCHEMA2.TEST");
			sa.execute("DROP SCHEMA IF EXISTS SCHEMA2");
		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests that a table in a non-default schema is dropped succussfully from the schema manager.
	 */
	@Test
	public void TestSchemaManagerDrop(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		
		try{

			sa.execute("CREATE SCHEMA SCHEMA2");
			sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("PUBLIC", rs.getString(2));
			} 
			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("SCHEMA2", rs.getString(2));
			} else {
				fail("Expected a schema manager entry here.");
			}

			/*
			 * Drop the table and check the result of the update
			 */
			sa.execute("DROP TABLE SCHEMA2.TEST");

			int result = sa.getUpdateCount();
			if (result != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Now check that the schema manager has correct information.
			 */
			sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");
			rs = sa.getResultSet();	
			
			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("PUBLIC", rs.getString(2));
			} 
			if (rs.next()){
				fail("There should only be one entry here.");
			} 

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}
	
	/**
	 * Tests that a non-default schema is dropped succussfully from the schema manager.
	 */
	@Test
	public void TestSchemaManagerDropSchema(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("CREATE SCHEMA SCHEMA2");
			sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");

			ResultSet rs = sa.getResultSet();

			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("PUBLIC", rs.getString(2));
			} 
			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("SCHEMA2", rs.getString(2));
			} else {
				fail("Expected a schema manager entry here.");
			}

			/*
			 * Drop the table and check the result of the update
			 */

			sa.execute("DROP TABLE IF EXISTS SCHEMA2.TEST");
			
			sa.execute("DROP SCHEMA SCHEMA2");

			int result = sa.getUpdateCount();
			if (result != 0){
				fail("Expected update count to be '0'");
			}

			/*
			 * Now check that the schema manager has correct information.
			 */
			sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");
			rs = sa.getResultSet();	
			
			if (rs.next()){
				assertEquals("TEST", rs.getString(1));
				assertEquals("PUBLIC", rs.getString(2));
			} 
			if (rs.next()){
				fail("There should only be one entry here.");
			} 

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Tests the feature to create multiple replicas at the same time.
	 */
	@Test
	public void CreateMultipleTestTablesLocal(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("CREATE SCHEMA SCHEMA2");
			sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO SCHEMA2.TEST VALUES(4, 'Meh');");
			sa.execute("INSERT INTO SCHEMA2.TEST VALUES(5, 'Heh');");

			//			sb.execute("CREATE REPLICA TEST, TEST2;");
			//
			//			if (sb.getUpdateCount() != 0){
			//				fail("Expected update count to be '0'");
			//			}


			sa.execute("SELECT LOCAL ONLY * FROM SCHEMA2.TEST ORDER BY ID;");

			int[] pKey = {4, 5};
			String[] secondCol = {"Meh", "Heh"};

			validateResults(pKey, secondCol, sa.getResultSet());

			sa.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

			int[] pKey2 = {1, 2};
			String[] secondCol2 = {"Hello", "World"};

			validateResults(pKey2, secondCol2, sa.getResultSet());

			sa.execute("DROP ALL OBJECTS");

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}

	/**
	 * Creates a new TEST table in a different schema, then creates remote replicas for each. Tested for
	 * success by accessing these remote replicas.
	 */
	@Test
	public void CreateMultipleTestReplicas(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		
		try{

			sa.execute("CREATE SCHEMA SCHEMA2");
			sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO SCHEMA2.TEST VALUES(4, 'Meh');");
			sa.execute("INSERT INTO SCHEMA2.TEST VALUES(5, 'Heh');");

			sb.execute("CREATE REPLICA TEST, SCHEMA2.TEST;");

			if (sb.getUpdateCount() != 0){
				fail("Expected update count to be '0'");
			}

			sb.execute("SELECT LOCAL ONLY * FROM SCHEMA2.TEST ORDER BY ID;");

			int[] pKey = {4, 5};
			String[] secondCol = {"Meh", "Heh"};

			validateResults(pKey, secondCol, sb.getResultSet());

			sb.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

			int[] pKey2 = {1, 2};
			String[] secondCol2 = {"Hello", "World"};

			validateResults(pKey2, secondCol2, sb.getResultSet());

			sa.execute("DROP ALL OBJECTS");
			sb.execute("DROP ALL OBJECTS");

		} catch (SQLException e){
			e.printStackTrace();
			fail("An Unexpected SQLException was thrown.");
		}
	}


	/**
	 * Tries to access SCHEMA2.TEST, where the SCHEMA2 schema does not exist. This should return an error, 
	 * rather than finding the TEST table in the default schema.
	 */
	@Test
	public void TestTableDoesntExist(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("SELECT * FROM SCEMA2.TEST");

			if (sa.getUpdateCount() == 0){
				fail("Expected a failure");
			}

		} catch (SQLException e){
			//expected.
		}
	}

	/**
	 * Tries to create a repica for SCHEMA2.TEST, where the SCHEMA2 schema does not exist. This should return an error, 
	 * rather than finding the TEST table in the default schema.
	 */
	@Test
	public void TestTableDoesntExistForReplica(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sb.execute("CREATE REPLICA SCEMA2.TEST;");

			if (sb.getUpdateCount() == 0){
				fail("Expected a failure");
			}

		} catch (SQLException e){
			//expected.
		}

	}

	/**
	 * Tries to create a replica of a schema that doesn't exist. Should fail.
	 */
	@Test
	public void TestTableDoesntExistForReplica2(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sb.execute("CREATE REPLICA SCEMA2;");

			if (sb.getUpdateCount() == 0){
				fail("Expected a failure");
			}

		} catch (SQLException e){
			//expected.
		}

	}
	
	/**
	 * Tests replication of an entire schema of tables.
	 */
	@Test
	public void ReplicateSchema(){
		Diagnostic.traceNoEvent(Diagnostic.FULL, "STARTING TEST");
		try{

			sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sa.execute("INSERT INTO TEST2 VALUES(4, 'Meh');");
			sa.execute("INSERT INTO TEST2 VALUES(5, 'Heh');");

			sb.execute("CREATE REPLICA SCHEMA PUBLIC;");

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
			fail("An Unexpected SQLException was thrown.");
		}
	}
}
