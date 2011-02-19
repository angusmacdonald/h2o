/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.run;

import java.io.File;

import org.h2o.test.BenchmarkTests;
import org.h2o.test.PreparedStatementTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite class which runs all unit tests that are in some way related to the operations typically performed by benchmarking tools.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({PreparedStatementTests.class, BenchmarkTests.class})
public class BenchmarkRelatedTests {

    // The above annotations do all the work.
    public static final String TEST_DESCRIPTOR_FILE = "service" + File.separator + "testDB.h2o";
}
