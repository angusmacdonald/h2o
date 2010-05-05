package org.h2.test.h2o;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.h2o.remote.ChordRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
import org.h2.h2o.util.locator.H2OLocatorInterface;
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
public class LocatorDatabaseTests extends TestBase {

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

	
//	/**
//	 * Tests that when the Table Manager is migrated another database instance is able to connect to the new manager without any manual intervention.
//	 * 
//	 */
//	@Test
//	public void TableManagerMigration() throws InterruptedException {
//		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");
//		try {
//			sas[1].executeUpdate("MIGRATE TABLEMANAGER test");
//
//			/*
//			 * Test that the new Table Manager can be found.
//			 */
//			sas[1].executeUpdate("INSERT INTO TEST VALUES(4, 'helloagain');");
//			
//			/*
//			 * Test that the old Table Manager is no longer accessible, and that the referene can be updated.
//			 */
//			sas[0].executeUpdate("INSERT INTO TEST VALUES(5, 'helloagainagain');");
//			
//		} catch (SQLException e) {
//			e.printStackTrace();
//			fail("Didn't work.");
//		}
//	}
	
	/**
	 * One node gets a majority while another backs out then tries again.
	 */
	@Test
	public void noMajorityForOneNode(){

	}

	/**
	 * Each node gets exactly half of the locks required to create a schema manager. This checks that one of the nodes
	 * eventually gets both locks.
	 */
	@Test
	public void twoLocatorsEachProcessStuckOnOneLock(){

	}


	/**
	 * Codenamed Problem B.
	 * 
	 * Connect to existing Database instance with ST state, but find no ST running.
	 */
	@Test
	public void instancesRunningButNoSystemTable(){

	}
	
}
