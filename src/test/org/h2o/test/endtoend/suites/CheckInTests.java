/*
 * Dynamic Java Compiler Wrapper Library Copyright (C) 2003-2008 Distributed Systems Architecture Research Group
 * http://www-systems.cs.st-andrews.ac.uk/ This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later
 * version. This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy
 * of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test.endtoend.suites;

import org.h2o.test.endtoend.normal.EndToEndDiskTests;
import org.h2o.test.endtoend.normal.EndToEndMemoryTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite that defines which end-to-end tests should be run on check-in.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({

EndToEndDiskTests.class, EndToEndMemoryTests.class})
public class CheckInTests {
    // Empty.
}
