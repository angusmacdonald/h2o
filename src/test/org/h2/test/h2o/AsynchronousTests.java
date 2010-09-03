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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;

import org.h2.table.Table;
import org.h2o.autonomic.settings.Settings;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.ErrorHandling;



/**
 * Class which tests the asynchronous query functionality of H2O.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class AsynchronousTests extends MultiProcessTestBase {

	/**
	 * Tests that an update can complete with only two machines.
	 * @throws InterruptedException
	 */
	@Test(timeout=30000)
	public void basicAsynchronousUpdate() throws InterruptedException{
		String create1 = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)); " +
		"INSERT INTO TEST VALUES(1, 'Hello'); INSERT INTO TEST VALUES(2, 'World');";

		try {
			
			killDatabase(2);
			
			sleep(5000);
			
			delayQueryCommit(2);
			
			startDatabase(2);
			
			sleep("About to create recreate connections to the newly restarted database.", 2000);
			
			createConnectionsToDatabase(2);
			
			executeUpdateOnNthMachine(create1, 0);
			
			sleep(1000);
			/*
			 * Create test table.
			 */
			assertTestTableExists(2, 0);
			assertMetaDataExists(connections[0], 1);

			sleep(2000);

			String createReplica = "CREATE REPLICA TEST;";
			executeUpdateOnNthMachine(createReplica, 1);
			executeUpdateOnNthMachine(createReplica, 2);

			sleep("Wait for create replica commands to execute.", 3000);

			String update = "INSERT INTO TEST VALUES(3, 'Third');";
			
			executeUpdateOnNthMachine(update, 0);
			
			assertTrue(assertTestTableExists(connections[0], 3));
			assertTrue(assertTestTableExists(connections[1], 3));
			assertTrue(assertTestTableExists(connections[2], 3));
		} catch (SQLException e) {
			e.printStackTrace();
			fail("Unexpected exception.");
		}
	}

	public static void pauseThreadIfTestingAsynchronousUpdates(Table table, Settings databaseSettings) {
		
		if (databaseSettings == null) {
			return;
		}
		
		if (!table.getName().equals("TEST")){
			return;
		}
		
		boolean delay = Boolean.parseBoolean(databaseSettings.get("DELAY_QUERY_COMMIT"));
		
		if (delay){
			try {
				ErrorHandling.errorNoEvent("Delay");
				
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
