package org.h2.test.h2o;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.h2o.remote.ChordRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.h2.h2o.util.locator.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;


/**
 * Class which conducts tests on 10 in-memory databases running at the same time.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ChordTests extends TestBase {

	private Statement[] sas;
	private DatabaseThread[] dts;
	private LocatorServer ls;
	private static String[] dbs =  {"two", "three"}; //, "four", "five", "six", "seven", "eight", "nine"
	
	/**
	 * Whether the System Table state has been replicated yet.
	 */
	public static boolean isReplicated = false;
	
	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_TEST = true;
		Constants.IS_NON_SM_TEST = false;

		setReplicated(false);
	}

	public static synchronized void setReplicated(boolean b){
		isReplicated = b;
	}

	public static synchronized boolean isReplicated(){
		return isReplicated;
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		
		Constants.IS_TEAR_DOWN = false; 
		
		//Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		//PersistentSystemTable.USERNAME = "angus";
		//PersistentSystemTable.PASSWORD = "";

		org.h2.Driver.load();

//		for (String db: dbs){
//
//			H2oProperties knownHosts = new H2oProperties(DatabaseURL.parseURL("jdbc:h2:mem:" + db), "instances");
//			knownHosts.createNewFile();
//			knownHosts.setProperty("jdbc:h2:sm:mem:one", ChordRemote.currentPort + "");
//			knownHosts.saveAndClose();
//
//		}

		TestBase.setUpDescriptorFiles();
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();
		
		dts = new DatabaseThread[dbs.length + 1];
		dts[0] = new DatabaseThread("jdbc:h2:sm:mem:one");
		dts[0].start();

		Thread.sleep(5000);

		for (int i = 1; i < dts.length; i ++){
			
			dts[i] = new DatabaseThread("jdbc:h2:mem:" + dbs[i-1]);
			dts[i].start();
			
			Thread.sleep(5000);
		}
		
		
		sas = new Statement[dbs.length + 1];

		for (int i = 0; i < dts.length; i ++){
			sas[i] = dts[i].getConnection().createStatement();
		}

		
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		sas[0].execute(sql);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		Constants.IS_TEAR_DOWN = true; 

		for (int i = 0; i < dts.length; i ++){
			dts[i].setRunning(false);
		}

		closeDatabaseCompletely();
		
		ls.setRunning(false);
		dts = null;
		sas = null;
		

		ls.setRunning(false);
		while (!ls.isFinished()){};
	}

	@Test
	public void baseTest(){
		try {
			sas[0].execute("SELECT * FROM TEST;");
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Failed to execute query.");
		}
	}
	
	/**
	 * This sequence of events used to lock the sys table causing entries to not be included in the System Table.
	 * This tests that it never happens again.
	 */
	@Test
	public void sysTableLock(){
		try {
			sas[1].execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sas[2].execute("CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			

			ResultSet rs = sas[0].executeQuery("SELECT * FROM H2O.H2O_TABLE");
			
			if (rs.next() && rs.next() && rs.next()){
				//pass
			} else {
				fail("Not enough results.");
			}
			
			if (rs.next()){
				fail("Too many results.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Failed to execute query.");
		}
	}
	
	/**
	 * Tests that when the Table Manager is migrated another database instance is able to connect to the new manager without any manual intervention.
	 * 
	 */
	@Test
	public void TableManagerMigration() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			sas[1].executeUpdate("MIGRATE TABLEMANAGER test");

			/*
			 * Test that the new Table Manager can be found.
			 */
			sas[1].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");
			
			/*
			 * Test that the old Table Manager is no longer accessible, and that the referene can be updated.
			 */
			sas[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");
			
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't work.");
		}
	}
	
	/**
	 * Tests that when migration fails when an incorrect table name is given.
	 */
	@Test
	public void TableManagerMigrationFail() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			sas[1].executeUpdate("MIGRATE TABLEMANAGER testy");
			fail("Didn't work.");
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
	}
	
	@Test
	public void TableManagerMigrationWithCachedReference() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			sas[0].executeUpdate("INSERT INTO TEST VALUES(7, '7');");
			sas[1].executeUpdate("INSERT INTO TEST VALUES(6, '6');");
			sas[2].executeUpdate("INSERT INTO TEST VALUES(8, '8');");
			
			sas[1].executeUpdate("MIGRATE TABLEMANAGER test");

			/*
			 * Test that the new Table Manager can be found.
			 */
			sas[2].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");
			
			/*
			 * Test that the old Table Manager is no longer accessible, and that the referene can be updated.
			 */
			sas[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");
			
			ResultSet rs = sas[0].executeQuery("SELECT manager_location FROM H2O.H2O_TABLE");
			
			if (rs.next()){
			assertEquals(2, rs.getInt(1));
			} else {
				fail("System Table wasn't updated correctly.");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't work.");
		}
	}
	
	
	/**
	 * Tests that when the System Table is migrated another database instance is able to connect to the new manager without any manual intervention.
	 * 
	 */
	@Test
	public void SystemTableMigration() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			sas[1].executeUpdate("MIGRATE SYSTEMTABLE");

			sas[2].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
			sas[2].execute("SELECT * FROM TEST2;");
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't work.");
		}
	}

	@SuppressWarnings("deprecation")
	@Test
	public void SystemTableFailure() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			
			dts[0].stop();
			sas[0].close();
			
			Thread.sleep(5000);
			
			sas[1].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't complete query");
		}
	}

	/**
	 * Tests that if there are a number of failures the schema is successfully moved on each time.
	 * @throws InterruptedException
	 */
	@Test
	public void FailureMultipleReinstantiation() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {

			sas[1].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			dts[0].stop();
			sas[0].close();
			
			Thread.sleep(5000);
			
			sas[1].executeUpdate("CREATE TABLE TEST3(ID INT PRIMARY KEY, NAME VARCHAR(255));");

			dts[0].stop();
			sas[0].close();
			
			Thread.sleep(5000);
			
			ResultSet rs = sas[1].executeQuery("SELECT * FROM H2O.H2O_TABLE");
			
			if (rs.next() && rs.next() && rs.next()){
				//pass
			} else {
				fail("Not enough results.");
			}
			
			if (rs.next()){
				fail("Too many results.");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't complete query");
		}
	}
	
	@Test
	public void FirstMachineDisconnect() throws InterruptedException {
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
		try {
			sas[0].close();
			dts[0].getConnection().close();
			
			Thread.sleep(5000);
			System.err.println("heere");
			sas[1].executeUpdate("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");

		} catch (SQLException e) {
			e.printStackTrace();
			fail("Didn't complete query");
		}
	}
	
//	/**
//	 * Tests that a Table Manager will migrate itself when the database is closed.
//	 * @throws InterruptedException
//	 */
//	@Test
//	public void TableManagerMigrationOnClose() throws InterruptedException {
//		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
//		try {
//			
//			sas[1].execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
//			
//			sas[1].close();
//			dts[1].getConnection().close();
//			
//			Thread.sleep(5000);
//			
//			sas[2].executeUpdate("INSERT INTO TEST VALUES(4, 'help');");
//
//			
//			ResultSet rs = sas[2].executeQuery("SELECT * FROM TEST WHERE ID=4");
//			
//			if (!rs.next()){
//				fail("Didn't add entry.");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//			fail("Didn't complete query");
//		}
//	}
}
