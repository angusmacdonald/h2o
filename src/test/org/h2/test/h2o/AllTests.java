package org.h2.test.h2o;

import org.h2.test.h2o.h2.H2Tests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite class which runs all H2O JUnit tests when started. 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	//Unit tests.
	//H2oProperties.class,
	TransactionNameTests.class,
	//Database Tests
	SystemTableTests.class,
	MultipleSchemaTests.class,
	ReplicaTests.class,
	CustomSettingsTests.class,
	IndexTests.class,
	UpdateTests.class,
	MultiQueryTransactionTests.class,
	H2Tests.class,
	WrapperTests.class,
	RestartTests.class,
	LocatorTests.class,
	//LocatorDatabaseTests.class,
	ChordTests.class
})
public class AllTests {
 //The above annotations do all the work.
}
