package org.h2.test.h2o;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import static org.junit.Assert.fail;

import org.h2.engine.Constants;
import org.h2.engine.SchemaManager;
import org.junit.After;
import org.junit.Before;

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
			sb.execute("DROP TABLE IF EXISTS TEST, TEST2");

			sa.close();
			sb.close();

			ca.close();	
			cb.close();	
		} catch (Exception e){
			e.printStackTrace();
			fail("Connections aren't bein closed correctly.");
		}
	}
}
