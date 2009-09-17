package org.h2.test.h2o;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite class which runs all H2O JUnit tests when started. 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	SchemaManagerTests.class,
	MultipleSchemaTests.class,
	ReplicaTests.class,
	IndexTests.class,
	UpdateTests.class
})
public class AllTests {
 //The above annotations do all the work.
}
