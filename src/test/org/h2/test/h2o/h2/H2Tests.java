package org.h2.test.h2o.h2;

import org.h2.test.h2o.H2SimpleTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite class which runs all H2 tests that have been converted to run with H2O.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
	H2SimpleTest.class,
	TestBigDb.class,
	TestBigResult.class
})
public class H2Tests {
 //The above annotations do all the work.
}
