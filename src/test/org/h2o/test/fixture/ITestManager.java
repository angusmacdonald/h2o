/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
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
package org.h2o.test.fixture;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import uk.ac.standrews.cs.nds.madface.exceptions.UnknownPlatformException;

import com.mindbright.ssh2.SSH2Exception;

public interface ITestManager {

    /**
     * Sets up the test.
     *
     * @throws IOException if fixture setup fails
     * @throws UnknownPlatformException  if fixture setup fails
     */
    void setUp() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException;

    /**
     * Tears down the test, removing persistent state.
     *
     * @throws SQLException if fixture tear-down fails
     */
    void tearDown() throws SQLException;

    void startup() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException;

    void shutdown();

    ConnectionDriver makeConnectionDriver(int db_index);
}
