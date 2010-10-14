/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests related to the functionality of the Table Manager.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableManagerTests {
	
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
	 * Tests that a new Table Manager is instantiated correctly when a table is created.
	 */
	@Test
	public void newTable() {
		fail("Not yet implemented.");
	}
	
	/**
	 * Tests that a new Table Manager is correctly notified when the new replica command is used.
	 */
	@Test
	public void newReplicaCommand() {
		fail("Not yet implemented.");
	}
	
	/**
	 * Tests that a new Table Manager is correctly notified when a new replica is created. This test actually creates a replica, while the
	 * previous newReplicaCommand test only runs the NEW REPLICA command.
	 */
	@Test
	public void newReplica() {
		fail("Not yet implemented.");
	}
	
	/**
	 * Tests that a new Table Manager instance is created when the machine holding the primary copy is restarted.
	 */
	@Test
	public void databaseRestart() {
		fail("Not yet implemented.");
	}
}
