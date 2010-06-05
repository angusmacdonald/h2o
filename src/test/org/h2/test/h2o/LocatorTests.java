package org.h2.test.h2o;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.h2o.remote.StartupException;
import org.h2.h2o.util.locator.H2OLocatorInterface;
import org.h2.h2o.util.locator.LocatorServer;
import org.h2.h2o.util.locator.LocatorState;
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
public class LocatorTests{

	/**
	 * Whether the System Table state has been replicated yet.
	 */
	public static boolean isReplicated = false;

	private static String[] replicaLocations = {"test1:databaseOnDisk", "test2:databaseOnDisk", "test3:databaseOnDisk", "test4:databaseOnDisk"};
	
	
	@BeforeClass
	public static void initialSetUp(){
		Diagnostic.setLevel(DiagnosticLevel.FULL);
		Constants.IS_TEST = true;
		Constants.IS_NON_SM_TEST = false;

	}

	private LocatorServer[] locatorServers;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {

		Constants.IS_TEAR_DOWN = false; 

		TestBase.setUpDescriptorFiles();

		locatorServers = new LocatorServer[4];
		for (int i = 0; i < locatorServers.length; i++){
			locatorServers[i] = new LocatorServer(20000+i, "junitLocator");
			locatorServers[i].createNewLocatorFile();
			locatorServers[i].start();
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() {
		Constants.IS_TEAR_DOWN = true; 

		for (int i = 0; i < locatorServers.length; i++){
			locatorServers[i].setRunning(false);
		}
		
		for (int i = locatorServers.length-1; i >= 0; i--){
			while (!locatorServers[i].isFinished()){};
		}
	}
	
	/**
	 * Test that the maths used to establish whether a majority has been achieved works
	 * as expected.
	 */
	@Test
	public void testMaths(){
		int number_successful = 1;
		int number_of_locators = 1;

		boolean result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertTrue(result);

		number_of_locators = 2;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertFalse(result);

		number_successful = 2;
		number_of_locators = 2;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertTrue(result);

		number_successful = 2;
		number_of_locators = 3;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertTrue(result);

		number_successful = 2;
		number_of_locators = 4;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertFalse(result);

		number_successful = 2;
		number_of_locators = 4;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertFalse(result);

		number_successful = 0;
		number_of_locators = 1;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertFalse(result);

		number_successful = 1;
		number_of_locators = 3;
		result = H2OLocatorInterface.hasAchievedMajority(number_successful, number_of_locators);

		assertFalse(result);
	}



	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * SINGLE LOCATOR TESTS
	 * 
	 * 
	 * 
	 * 
	 */
	/**
	 * Successfully obtained a lock.
	 */
	@Test
	public void getLock(){

	}

	/**
	 * Successfully wrote then obtained the set of database instances.
	 */
	@Test
	public void writeThenReadDatabaseInstances(){

		String[] locatorLocations = {"eigg:20000"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		boolean successful = false;

		successful = setThenGetLocations(replicaLocations, locatorInterface);

		assertTrue(successful);
	}

	/**
	 * Check that the server not being found is handled gracefully.
	 */
	@Test
	public void locatorServerNotFound(){
		String[] locatorLocations = {"eigg:12111"};
		H2OLocatorInterface locatorInterface = null;

		/*
		 * Set locations.
		 */
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);

			locatorInterface.setLocations(replicaLocations);

			fail("Expected failure.");
		} catch (IOException e) {

		}


	}


	/*
	 * 
	 * 
	 * 
	 * 
	 * 
	 * MULTIPLE LOCATOR TESTS
	 * 
	 * 
	 * 
	 * 
	 */

	/**
	 * Successfully wrote then obtained the set of database instances.
	 */
	@Test
	public void writeThenReadDatabaseInstancesMultipleLocators(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001", "eigg:20002"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}

		boolean successful = false;

		successful = setThenGetLocations(replicaLocations, locatorInterface);

		assertTrue(successful);
	}
	
	/**
	 * Writes to a number of replicas, where the majority have failed / don't exist.
	 */
	@Test
	public void writeDatabaseInstancesMultipleFailedLocators(){

		String[] locatorLocations = {"eigg:20000", "eigg:20007", "eigg:20008"};
		H2OLocatorInterface locatorInterface = null;

		boolean successful = false;

		/*
		 * Set locations.
		 */
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);

			successful = locatorInterface.setLocations(replicaLocations);
		} catch (IOException e) {
			e.printStackTrace();
			fail("Unexpected IOException when setting replica locations.");
		}

		/*
		 * Get locations.
		 */
		try {
			List<String> locations = locatorInterface.getLocations();

			assertEquals(replicaLocations.length, locations.size());

			for (int i = 0; i < replicaLocations.length; i++){
				if (!locations.contains(replicaLocations[i])){
					fail("Not all locations matched.");
				}
			}

		} catch (IOException e) {
			fail("Unexpected IOException when getting replica locations.");
		}

		assertTrue(successful);
	}

	/**
	 * Majority achieved.
	 */
	@Test
	public void majorityNoFailure(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001"};
		
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		
		boolean successful = setThenGetLocations(replicaLocations, locatorInterface);

		assertTrue(successful);
		
		/*
		 * Lock.
		 */
		try {
			successful = locatorInterface.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			e.printStackTrace();
			fail("Didn't achieve majority the hard way.");
		}

		assertTrue(successful);
	}
	
	/**
	 * Tries to get a lock out without having sent a get request. This should fail because it
	 * is an illegal action.
	 */
	@Test
	public void failureNoGetRequest(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		/*
		 * Lock.
		 */
		try {
			locatorInterface.lockLocators("databaseInstance");
			
			fail("Should have thrown a startup exception.");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			//Expected.
		}

	}

	/**
	 * Majority achieved, then unlocked successfully.
	 */
	@Test
	public void majorityThenCommit(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001", "eigg:20008"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		
		boolean successful = setThenGetLocations(replicaLocations, locatorInterface);


		/*
		 * Lock.
		 */
		try{
			successful = locatorInterface.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}

		assertTrue(successful);
		
		
		/*
		 * Commit (unlock).
		 */
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);

			successful = locatorInterface.commitLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		

		assertTrue(successful);
		
	}
	
	/**
	 * Try to unlock (commit) with the wrong database instance string.
	 */
	@Test
	public void commitFail(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001", "eigg:20008"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		
		boolean successful = setThenGetLocations(replicaLocations, locatorInterface);

		/*
		 * Lock.
		 */
		try {
			successful = locatorInterface.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}

		assertTrue(successful);
		
		
		/*
		 * Commit (unlock).
		 */
		try {

			successful = locatorInterface.commitLocators("databaseInstance22");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}


		assertFalse(successful);
		
	}
	
	/**
	 * Check that when the update count doesn't match a lock cannot be taken out.
	 */
	@Test
	public void commitCheckUpdateCount(){

		String[] locatorLocations = {"eigg:20000", "eigg:20001", "eigg:20002"};
		String[] replicaLocations = {"test1:databaseOnDisk", "test2:databaseOnDisk", "test3:databaseOnDisk", "test4:databaseOnDisk"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		
		boolean successful = setThenGetLocations(replicaLocations, locatorInterface);

		assertTrue(successful);
		
		H2OLocatorInterface locatorInterfaceTwo = null;
		try {
			locatorInterfaceTwo = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		successful = setThenGetLocations(replicaLocations, locatorInterfaceTwo);
		assertTrue(successful);
		
		/*
		 * Lock then commit the second 'instance'.
		 */
		
		try {
			successful = locatorInterfaceTwo.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		
		try {
			successful = locatorInterfaceTwo.commitLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		

		/*
		 * Lock the first instance.
		 */
		try {
			successful = locatorInterface.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		
		assertFalse(successful);
		
		/*
		 * This should fail because a commit action has taken place since the first 'get' of the db locations.
		 * 
		 * The 'instance' must still manually unlock, or wait for a timeout.
		 */
		
	}

	/**
	 * Codenamed Problem A.
	 * 
	 * Majority achieved, but node fails before it can create a system table.
	 * @throws InterruptedException 
	 */
	@Test
	public void majorityThenFailure() throws InterruptedException{
		String[] locatorLocations = {"eigg:20000", "eigg:20001", "eigg:20002"};
		String[] replicaLocations = {"test1:databaseOnDisk", "test2:databaseOnDisk", "test3:databaseOnDisk", "test4:databaseOnDisk"};
		H2OLocatorInterface locatorInterface = null;
		try {
			locatorInterface = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		
		boolean successful = setThenGetLocations(replicaLocations, locatorInterface);

		assertTrue(successful);
		
		H2OLocatorInterface locatorInterfaceTwo = null;
		try {
			locatorInterfaceTwo = new H2OLocatorInterface(locatorLocations);
		} catch (IOException e) {
			fail("Unexpected exception.");
		}
		successful = setThenGetLocations(replicaLocations, locatorInterfaceTwo);
		assertTrue(successful);
		
		
		/*
		 * Lock the first instance.
		 */
		try {
			successful = locatorInterface.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		
		
		/*
		 * Try to get a lock with the second locator. This will fail.
		 */
		
		try {
			successful = locatorInterfaceTwo.lockLocators("databaseInstanceTwo");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		
		assertFalse(successful);
		
		/*
		 * Sleep then try to get a lock again. The timeout should have kicked in.
		 */
		
		Thread.sleep(LocatorState.LOCK_TIMEOUT + 500);
		
		try {
			successful = locatorInterfaceTwo.lockLocators("databaseInstance");
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		} catch (StartupException e) {
			fail("Didn't achieve majority the hard way.");
		}
		
		assertTrue(successful);
	}


	/**
	 * @param locatorLocations
	 * @param replicaLocations
	 * @param locatorInterface
	 * @param successful
	 * @return
	 */
	private boolean setThenGetLocations(String[] replicaLocations, H2OLocatorInterface locatorInterface) {
		boolean successful = false;
		
		/*
		 * Set locations.
		 */
		try {

			successful = locatorInterface.setLocations(replicaLocations);
		} catch (IOException e) {
			fail("Unexpected IOException when setting replica locations.");
		}

		/*
		 * Get locations.
		 */
		try {
			List<String> locations = locatorInterface.getLocations();

			assertEquals(replicaLocations.length, locations.size());

			for (int i = 0; i < replicaLocations.length; i++){
				if (!locations.contains(replicaLocations[i])){
					fail("Not all locations matched.");
				}
			}

		} catch (IOException e) {
			fail("Unexpected IOException when getting replica locations.");
		}
		return successful;
	}
}