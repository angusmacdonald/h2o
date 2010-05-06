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
	private static String[] dbs =  {"two", "three", "four"}; //, "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen"};
		//"sixteen", "seventeen", "eighteen", "nineteen", "twenty", "twenty-one", "twenty-one", "twenty-two", "twenty-three", "twenty-four", "twenty-five", "twenty-six", "twenty-seven"};

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
		
		org.h2.Driver.load();


		TestBase.setUpDescriptorFiles(dbs, "http://www.cs.st-andrews.ac.uk/~angus/databases/testDB.h2o", "testDB");
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();
		
		startDatabases();
		
		Thread.sleep(5000);
		
		sas = new Statement[dbs.length + 1];

		for (int i = 0; i < dts.length; i ++){
			sas[i] = dts[i].getConnection().createStatement();
		}

		

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		Constants.IS_TEAR_DOWN = true; 

		shutdownDatabases();

		ls.setRunning(false);
		
		closeDatabaseCompletely();
		
		dts = null;
		sas = null;
		

		
		while (!ls.isFinished()){};
	}


	/**
	 * 
	 */
	private void startDatabases() {
		dts = new DatabaseThread[dbs.length + 1];
		dts[0] = new DatabaseThread("jdbc:h2:sm:mem:one", true);
		dts[0].start();

		for (int i = 1; i < dts.length; i ++){
			
			dts[i] = new DatabaseThread("jdbc:h2:mem:" + dbs[i-1], true);
			dts[i].start();
		}
	}

	/**
	 * 
	 */
	private void shutdownDatabases() {
		for (int i = 0; i < dts.length; i ++){
			dts[i].setRunning(false);
		}
	}

	
	@Test
	public void createLotsInOneGo() throws InterruptedException{
		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
		sql += "INSERT INTO TEST VALUES(2, 'World');";

		for (int i = 0; i < dts.length; i ++){
			while (!dts[i].isConnected()){Thread.sleep(100);};
		}
		
		shutdownDatabases();
		
		for (int i = 0; i < dts.length; i ++){
			while (dts[i].isAlive()){Thread.sleep(100);};
		}
		
		startDatabases();
			
		for (int i = 0; i < dts.length; i ++){
			while (!dts[i].isConnected()){Thread.sleep(100);};
		}
		
		sas = new Statement[dbs.length + 1];
		for (int i = 0; i < dts.length; i ++){
			try {
				sas[i] = dts[i].getConnection().createStatement();
			} catch (Exception e) {
				fail("Couldn't get a database connection.");
			}
			try {
				sas[i].executeUpdate("INSERT INTO TEST VALUES(" + (i+4) + ", Value);");
			} catch (SQLException e) {
				fail("Failed to insert into database.");
			}
		}
		
	}
	
	/**
	 * One node gets a majority while another backs out then tries again.
	 * @throws InterruptedException 
	 */
	@Test
	public void noMajorityForOneNode() throws InterruptedException{
	
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
