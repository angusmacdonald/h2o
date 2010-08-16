/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2.test.h2o;

import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2o.autonomic.settings.Settings;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Various tests for replication in H2O. The 'CREATE REPLICA' function in particular.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class CustomSettingsTests extends TestBase{

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		Constants.IS_TEAR_DOWN = false; 
		setUpDescriptorFiles();
		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		/*
		 * Alter Settings file.
		 */

		Properties settings = Settings.defaultSettings();
		settings.setProperty("RELATION_REPLICATION_FACTOR", "2");

		String databaseName = "jdbc:h2:mem:one";
		Settings.saveAsLocalProperties(settings, databaseName);


		org.h2.Driver.load();

		ca = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
		cb = DriverManager.getConnection("jdbc:h2:mem:two", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

		sa = ca.createStatement();
		sb = cb.createStatement();
	}


	/**
	 * Tests that a replica is automatically created on B when replication factor is set to 2.
	 * @throws SQLException 
	 * @throws InterruptedException 
	 */
	@Test
	public void ReplicaAutomaticallyCreated() throws SQLException, InterruptedException{
		Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "STARTING TEST");


		try{
			String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
			sql += "INSERT INTO TEST VALUES(1, 'Hello');";
			sql += "INSERT INTO TEST VALUES(2, 'World');";

			sa.execute(sql);
			sa.execute("INSERT INTO TEST VALUES(3, 'Hello World');");

			int[] pKey = {1, 2, 3};
			String[] secondCol = {"Hello", "World", "Hello World"};

			validateOnFirstMachine("TEST", pKey, secondCol);
			validateOnSecondMachine("TEST", pKey, secondCol);

		} catch (SQLException e){
			e.printStackTrace();
			fail("This should succeed.");
		}
	}

}
