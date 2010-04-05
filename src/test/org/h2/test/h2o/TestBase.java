package org.h2.test.h2o;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.h2o.remote.ChordRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

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

	/**
	 * The number of rows that are in the test table after the initial @see {@link #setUp()} call.
	 */
	protected static final int ROWS_IN_DATABASE = 2;

	@BeforeClass
	public static void initialSetUp(){
		
		Constants.IS_NON_SM_TEST = true;
		
		Diagnostic.setLevel(DiagnosticLevel.FULL);

		H2oProperties properties = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("schemaManagerLocation", "jdbc:h2:sm:mem:one");

		properties.saveAndClose();

		properties = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:three"));

		properties.createNewFile();
		//"jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test"
		properties.setProperty("schemaManagerLocation", "jdbc:h2:sm:mem:one");

		properties.saveAndClose();
		



	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		H2oProperties knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordRemote.currentPort + "");
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:two"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordRemote.currentPort + "");
		knownHosts.saveAndClose();
		
		knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:three"), "instances");
		knownHosts.createNewFile();
		knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordRemote.currentPort + "");
		knownHosts.saveAndClose();
		
		//Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		//PersistentSchemaManager.USERNAME = "sa";
		//PersistentSchemaManager.PASSWORD = "sa";

		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
		cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

		sa = ca.createStatement();
		sb = cb.createStatement();

		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		sa.execute(sql);
	}

	/**
	 * @throws SQLException 
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws SQLException {
		try{
			//			sa.execute("DROP TABLE IF EXISTS TEST");
			//			sb.execute("DROP TABLE IF EXISTS TEST");
			sa.execute("DROP ALL OBJECTS");
			sb.execute("DROP ALL OBJECTS");

			if (!sa.isClosed()) sa.close();
			if (!sb.isClosed())sb.close();

			if (!ca.isClosed()){
				ca.close();	
			}
			if (!cb.isClosed())cb.close();	

			closeDatabaseCompletely();

		} catch (Exception e){
			e.printStackTrace();
		}
		ca = null;
		cb = null;
		sa = null;
		sb = null;
	}

	/**
	 * Close the database explicitly, in case it didn't shut down correctly between tests.
	 */
	public static void closeDatabaseCompletely() {
		obliterateRMIRegistyContents();
		Collection<Database> dbs = Engine.getInstance().closeAllDatabases();

		for (Database db: dbs){
			db.close(false);
			db.shutdownImmediately();
		}
		
		dbs = null;

	}

	/**
	 * Removes every object from the RMI registry.
	 */
	private static void obliterateRMIRegistyContents(){
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
					if (!l.equals("IChordNode")){
						registry.unbind(l);
					}
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
			if (pKey[i] != 0 && secondCol[i] != null){ //indicates the entry was deleted as part of the test.
				if (rs.next()){
					assertEquals(pKey[i], rs.getInt(1));
					assertEquals(secondCol[i], rs.getString(2));

				} else {
					fail("Expected an entry here.");
				}
			}
		}

		if (rs.next()){
			fail("Too many entries.");
		}

		rs.close();
	}

	/**
	 * Create a replica on the second test database.
	 * @throws SQLException
	 */
	protected void createReplicaOnB() throws SQLException {
		/*
		 * Create replica on B.
		 */
		sb.execute("CREATE REPLICA TEST;");

		if (sb.getUpdateCount() != 0){
			fail("Expected update count to be '0'");
		}
	}


	/**
	 * Validate the result of a query on the first replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 */
	protected void validateOnFirstMachine(TestQuery testQuery)
	throws SQLException {
		validateOnFirstMachine(testQuery.getTableName(), testQuery.getPrimaryKey(), testQuery.getSecondColumn());
	}

	/**
	 * Validate the result of a query on the second replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry
	 */
	protected void validateOnSecondMachine(TestQuery testQuery)
	throws SQLException {
		validateOnSecondMachine(testQuery.getTableName(), testQuery.getPrimaryKey(), testQuery.getSecondColumn());
	}

	/**
	 * Validate the result of a query on the second replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 * @param pKey			Primary key value
	 * @param secondCol		Second column value in test table.
	 * @throws SQLException
	 */
	protected void validateOnSecondMachine(String tableName, int[] pKey, String[] secondCol)
	throws SQLException {
		sb.execute("SELECT LOCAL * FROM " + tableName + " ORDER BY ID;"); 
		validateResults(pKey, secondCol, sb.getResultSet());
	}


	/**
	 * Validate the result of a query on the first replica against expected values by selecting
	 * everything in a table sorted by ID and comparing with each entry.
	 * @param pKey			Primary key value
	 * @param secondCol		Second column value in test table.
	 * @throws SQLException
	 */
	protected void validateOnFirstMachine(String tableName, int[] pKey, String[] secondCol)
	throws SQLException {
		sa.execute("SELECT LOCAL * FROM " + tableName + " ORDER BY ID;"); 
		validateResults(pKey, secondCol, sa.getResultSet());
	}

	/**
	 * @param stat 
	 * @param tableName 
	 * @throws SQLException
	 */
	protected void createSecondTable(Statement stat, String tableName) throws SQLException {
		String sqlQuery = "CREATE TABLE " + tableName + "(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sqlQuery += "INSERT INTO " + tableName + " VALUES(4, 'Meh');";
		sqlQuery += "INSERT INTO " + tableName + " VALUES(5, 'Heh');";

		stat.execute(sqlQuery);
	}

}
