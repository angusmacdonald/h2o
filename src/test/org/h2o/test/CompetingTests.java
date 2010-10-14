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

import java.sql.SQLException;

import junit.framework.Assert;

import org.junit.Test;

/**
 * Tests that check the ability of the system to propagate updates, both in cases where the update can be committed, and in cases where it
 * must be aborted.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class CompetingTests extends TestBase {
	
	/**
	 * Tests the case of multiple database instances attempting to access a table at the same time. Exclusive access should be ensured
	 * during the period of writes.
	 * 
	 * <p>
	 * Numerous entries should cause failure, because of the lock contention.
	 */
	@Test
	public void testConcurrentQueriesCompetingUpdates1() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates2() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates3() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates4() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates5() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates6() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates7() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates8() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9a() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9b() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9v() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9ds() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdatessdvs() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9tjedf() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
	@Test
	public void testConcurrentQueriesCompetingUpdates9lkuu() {
		try {
			sb.execute("CREATE REPLICA TEST");
		} catch ( SQLException e1 ) {
			e1.printStackTrace();
			fail("This wasn't even the interesting part of the test.");
		}
		
		try {
			Thread.sleep(100);
		} catch ( InterruptedException e ) {
		}
		int entries = 100;
		ConcurrentTest cta = new ConcurrentTest(sa, 3, entries, true);
		ConcurrentTest ctb = new ConcurrentTest(sb, 3000, entries, true);
		new Thread(cta).start();
		
		ctb.run();
		
		boolean result = ctb.successful;
		
		Assert.assertEquals(true, result);
	}
	
}
