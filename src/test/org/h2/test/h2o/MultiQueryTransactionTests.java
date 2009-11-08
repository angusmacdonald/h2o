package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.junit.Test;


/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultiQueryTransactionTests extends TestBase{

	/**
	 * Attempts to insert a number of queries into the database in the same transaction. Update on a single replica.
	 */
	@Test
	public void basicMultiQueryInsert(){
		try{

			int rowsAlreadyInDB = 2;
			int totalIterations = 10;
			String sqlToExecute = "";


			int[] pKey = new int[totalIterations + rowsAlreadyInDB];
			String[] secondCol = new String[totalIterations + rowsAlreadyInDB];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";



			for (int i = (rowsAlreadyInDB+1); i < (totalIterations+(rowsAlreadyInDB+1)); i++){
				pKey[i-1] = i;
				secondCol[i-1] = "helloNumber" + i;
				
				sqlToExecute += "INSERT INTO TEST VALUES(" + pKey[i-1] + ", '" + secondCol[i-1] + "'); ";
			
			}

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

			int rowsAlreadyInDB = 2;
			int totalIterations = 10;
			String sqlToExecute = "";


			int[] pKey = new int[totalIterations + rowsAlreadyInDB];
			String[] secondCol = new String[totalIterations + rowsAlreadyInDB];

			pKey[0] = 1; pKey[1] = 2;
			secondCol[0] = "Hello"; secondCol[1] = "World";



			for (int i = (rowsAlreadyInDB+1); i < (totalIterations+(rowsAlreadyInDB+1)); i++){
				pKey[i-1] = i;
				secondCol[i-1] = "helloNumber" + i;
				
				sqlToExecute += "INSERT INTO TEST VALUES(" + pKey[i-1] + ", '" + secondCol[i-1] + "'); ";
			
			}

			System.out.println("About to do the big set of inserts:");
			sb.execute(sqlToExecute);

			sa.execute("SELECT PRIMARY * FROM TEST ORDER BY ID;"); 
			validateResults(pKey, secondCol, sa.getResultSet());


		} catch (SQLException sqle){
			sqle.printStackTrace();
			fail("SQLException thrown when it shouldn't have.");
		}
	}
}
