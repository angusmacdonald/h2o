package org.h2.test.h2o;


import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests related to the functionality of the data manager.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DataManagerTests {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Tests that a new data manager is instantiated correctly when a table is created.
	 */
	@Test
	public void newTable(){
		fail("Not yet implemented.");
	}

	/**
	 * Tests that a new data manager is correctly notified when the new replica command is used.
	 */
	@Test
	public void newReplicaCommand(){
		fail("Not yet implemented.");
	}
	
	/**
	 * Tests that a new data manager is correctly notified when a new replica is created. This test actually creates
	 * a replica, while the previous newReplicaCommand test only runs the NEW REPLICA command.
	 */
	@Test
	public void newReplica(){
		fail("Not yet implemented.");
	}
	
	/**
	 * Tests that a new data manager instance is created when the machine holding the primary copy is restarted.
	 */
	@Test
	public void databaseRestart(){
		fail("Not yet implemented.");
	}
}
