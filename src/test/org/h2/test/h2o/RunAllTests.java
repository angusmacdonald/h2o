package org.h2.test.h2o;


import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite class which runs all H2O JUnit tests when started. The class itself is empty.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	SchemaManagerTests.class,
	ReplicaTests.class
})
public class RunAllTests {
	//Tests are run with the above annotations.
}
