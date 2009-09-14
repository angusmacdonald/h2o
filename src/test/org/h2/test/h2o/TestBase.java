package org.h2.test.h2o;

import static org.junit.Assert.assertEquals;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import static org.junit.Assert.fail;

import org.h2.engine.Constants;
import org.h2.engine.Engine;
import org.h2.engine.SchemaManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import uk.ac.stand.dcs.nds.util.Diagnostic;

/**
 * Base class for JUnit tests. Performs a basic setup of two in-memory databases which are used for the rest of testing.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TestBase {
	Connection ca = null;
	Connection cb = null;
	Statement sa = null;
	Statement sb = null;

	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(Diagnostic.FULL);
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		SchemaManager.USERNAME = "sa";
		SchemaManager.PASSWORD = "sa";

		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", "sa", "sa");
		cb = DriverManager.getConnection("jdbc:h2:mem:two", "sa", "sa");

		sa = ca.createStatement();
		sb = cb.createStatement();

		sa.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
		sa.execute("INSERT INTO TEST VALUES(1, 'Hello');");
		sa.execute("INSERT INTO TEST VALUES(2, 'World');");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		try{ 
			sa.execute("DROP TABLE IF EXISTS TEST, TEST2");

			sa.execute("DROP SCHEMA IF EXISTS SCHEMA2");
			sb.execute("DROP SCHEMA IF EXISTS SCHEMA2");
						
			sa.close();
			sb.close();
			
			ca.close();	
			cb.close();	

			obliterateRMIRegistyContents();
			Engine.getInstance().closeAllDatabases();
			
		} catch (Exception e){
			e.printStackTrace();
			fail("Connections aren't being closed correctly.");
		}
	}
	

	public static void obliterateRMIRegistyContents(){
		Registry registry = null;
		
		try {
				registry = LocateRegistry.getRegistry(20000);

		} catch (RemoteException e) {
			e.printStackTrace();
		}

		try {
			String[] listOfObjects = registry.list();
			
			for (String l: listOfObjects){
				try {
					registry.unbind(l);
				} catch (NotBoundException e) {
					fail("Failed to remove " + l + " from RMI registry.");
				}
			}
			
			if (registry.list().length > 0){
				fail("Somehow failed to empty RMI registry.");
			}
		} catch (Exception e) {
			//It happens for tests where the registry was not set up.
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
	public void validateResults(int[] pKey, String[] secondCol, ResultSet rs) throws SQLException {
		if (rs == null)
			fail("Resultset was null. Probably an incorrectly set test.");

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
