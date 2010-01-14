package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.engine.SchemaManager;
import org.h2.h2o.util.DatabaseURL;
import org.h2.h2o.util.H2oProperties;
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

	private Connection[] cas;
	private Statement[] sas;
	
	private static String[] dbs = {"two", "three", "four", "five", "six", "seven", "eight", "nine"};

	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_TEST = true;

		createMultiplePropertiesFiles(dbs);


	}


	private static void createMultiplePropertiesFiles(String[] dbNames){

		for (String db: dbNames){

			String fullDBName = "jdbc:h2:mem:" + db;

			H2oProperties properties = new H2oProperties(DatabaseURL.parseURL(fullDBName));

			properties.createNewFile();

			properties.setProperty("schemaManagerLocation", "jdbc:h2:sm:mem:one");

			properties.saveAndClose();
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		//Constants.DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:mem:one";
		SchemaManager.USERNAME = "sa";
		SchemaManager.PASSWORD = "sa";

		org.h2.Driver.load();

		cas = new Connection[dbs.length + 1];
		cas[0] = DriverManager.getConnection("jdbc:h2:sm:mem:one", "sa", "sa");
		for (int i = 1; i < cas.length; i ++){
			cas[i] = DriverManager.getConnection("jdbc:h2:mem:" + dbs[i-1], "sa", "sa");
		}

		sas = new Statement[dbs.length + 1];

		for (int i = 0; i < cas.length; i ++){
			sas[i] = cas[i].createStatement();
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



		closeDatabaseCompletely();

		cas = null;
		sas = null;
	}

	@Test
	public void nullTest(){
	
	}
}
