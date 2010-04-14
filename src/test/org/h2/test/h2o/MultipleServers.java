package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.h2o.manager.PersistentSystemTable;
import org.h2.h2o.remote.ChordRemote;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * When an instance of this class is created 9 H2O in-memory instances are also created. These can then
 * be used in testing.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultipleServers {
	private Connection[] cas;
	private Statement[] sas;

	private String[] dbs = {"two", "three", "four", "five", "six", "seven", "eight", "nine"};


	public MultipleServers(){
		initialSetUp();

		try {
			setUp();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);


		createMultiplePropertiesFiles(dbs);


	}

	private void createMultiplePropertiesFiles(String[] dbNames){
		ChordRemote.currentPort = 30003;
		for (String db: dbNames){

			String fullDBName = "jdbc:h2:mem:" + db;
			DatabaseURL dbURL = DatabaseURL.parseURL(fullDBName);

			H2oProperties properties = new H2oProperties(dbURL);
			properties.createNewFile();
			properties.setProperty("systemTableLocation", "jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db");
			properties.saveAndClose();

			H2oProperties knownHosts = new H2oProperties(dbURL, "instances");
			knownHosts.createNewFile();
			knownHosts.setProperty("jdbc:h2:sm:tcp://localhost:9090/db_data/one/test_db", "30000"); // //jdbc:h2:sm:mem:one
			knownHosts.saveAndClose();

		}
	}


	public void setUp() throws Exception {
		//Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		//PersistentSystemTable.USERNAME = "angus";
		//PersistentSystemTable.PASSWORD = "";

		org.h2.Driver.load();

		
		
		cas = new Connection[dbs.length + 1];
		//cas[0] = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		for (int i = 1; i < cas.length; i ++){
			cas[i] = DriverManager.getConnection("jdbc:h2:mem:" + dbs[i-1], PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		}

		sas = new Statement[dbs.length + 1];

//		for (int i = 0; i < cas.length; i ++){
//			sas[i] = cas[i].createStatement();
//		}

//		String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
//		sql += "INSERT INTO TEST VALUES(1, 'Hello');";
//		sql += "INSERT INTO TEST VALUES(2, 'World');";
//
//		sas[0].execute(sql);

	}

	public void tearDown() {

		for (int i = 0; i < sas.length; i ++){
			try{ 
				if (!sas[i].isClosed())sas[i].close();	
				sas[i] = null;
			} catch (Exception e){
				e.printStackTrace();
				fail("Statements aren't being closed correctly.");
			}
		}

		for (int i = 0; i < cas.length; i ++){
			try{ 
				if (!cas[i].isClosed())cas[i].close();	
				cas[i] = null;
			} catch (Exception e){
				e.printStackTrace();
				fail("Connections aren't being closed correctly.");
			}
		}

		cas = null;
		sas = null;
	}

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		Constants.IS_TEST = false;
		MultipleServers servers = new MultipleServers();
		
	//	Thread.sleep(2000);
		
		//servers.testSystemTableFailure();
		
	//	Thread.sleep(2000);
		
		//servers.insertSecondTable();
	}

	/**
	 * 
	 */
	private void insertSecondTable() {
		try {
			sas[1].execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 */
	private void testSystemTableFailure() {
		Diagnostic.trace("CLOSING System Table INSTANCE");
		
		try {
			cas[0].close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
