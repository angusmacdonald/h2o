/*
 * Dynamic Java Compiler Wrapper Library Copyright (C) 2003-2008 Distributed Systems Architecture Research Group
 * http://www-systems.cs.st-andrews.ac.uk/ This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy
 * of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.run;

import org.h2o.test.SystemTableTests;
import org.h2o.test.TransactionNameTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests run on each build.
 */
@RunWith(Suite.class)
//@Suite.SuiteClasses({MultipleSchemaTests.class, ReplicaTests.class, CustomSettingsTests.class, IndexTests.class, UpdateTests.class, MultiQueryTransactionTests.class, H2Tests.class, WrapperTests.class, RestartTests.class, LocatorTests.class, AsynchronousTests.class})
@Suite.SuiteClasses({TransactionNameTests.class, SystemTableTests.class})
public class CheckInTests {
    // Empty.
}
