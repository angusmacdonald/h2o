package org.h2.test.h2o;

import org.h2.h2o.util.H2oProperties;
import org.h2.h2o.util.TransactionNameGenerator;
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
	H2oProperties.class,
	TransactionNameGenerator.class,
	//Database Tests
	SchemaManagerTests.class,
	MultipleSchemaTests.class,
	ReplicaTests.class,
	IndexTests.class,
	UpdateTests.class,
	MultiQueryTransactionTests.class, 
	RestartTests.class,
	H2SimpleTest.class,
	ChordTests.class
})
public class AllTests {
 //The above annotations do all the work.
}
