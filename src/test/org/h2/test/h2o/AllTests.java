/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2.test.h2o;

import java.io.File;

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
	ChordTests.class,
	FailureTests.class
})

public class AllTests {
	//The above annotations do all the work.
	public static final String TEST_DESCRIPTOR_FILE = "service" + File.separator + "testDB.h2o";
}
