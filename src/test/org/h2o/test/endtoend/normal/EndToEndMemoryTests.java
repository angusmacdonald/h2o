/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database.                       *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/
package org.h2o.test.endtoend.normal;

import java.io.IOException;
import java.sql.SQLException;

import org.h2o.test.fixture.IMemoryConnectionDriverFactory;
import org.h2o.test.fixture.ITestManager;
import org.h2o.test.fixture.MemoryConnectionDriverFactory;
import org.h2o.test.fixture.MemoryTestManager;
import org.junit.Ignore;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;

/**
 * User-oriented tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndMemoryTests extends EndToEndTests {

    private final IMemoryConnectionDriverFactory connection_driver_factory = new MemoryConnectionDriverFactory();

    private final ITestManager test_manager = new MemoryTestManager(1, connection_driver_factory);

    @Override
    public ITestManager getTestManager() {

        return test_manager;
    }

    @Override
    @Test
    @Ignore
    public void persistence() throws SQLException, IOException, UnknownPlatformException {

        // Not applicable to in-memory database.
    }

    @Override
    @Test
    @Ignore
    public void rollbackWithoutAutoCommit() throws SQLException, IOException, UnknownPlatformException {

        // Not applicable to in-memory database.
    }
}