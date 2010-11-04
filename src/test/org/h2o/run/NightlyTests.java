/*
 * Dynamic Java Compiler Wrapper Library Copyright (C) 2003-2008 Distributed Systems Architecture Research Group
 * http://www-systems.cs.st-andrews.ac.uk/ This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy
 * of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.run;

import org.h2o.test.FailureTests;
import org.h2o.test.h2.H2TestsFailing;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests run nightly.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({H2TestsFailing.class, FailureTests.class})
//, FailingEndToEndTests.class})
//, H2OTest.class, IndexTests.class, LocatorDatabaseTests.class, ReplicaTests.class, ReplicaTests.class, TableManagerTests.class, CustomSettingsTests.class,
//IndexTests.class, UpdateTests.class, MultiQueryTransactionTests.class, H2Tests.class, WrapperTests.class, RestartTests.class, LocatorTests.class, AsynchronousTests.class})
public class NightlyTests {
    // Empty.
}
