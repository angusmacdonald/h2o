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

import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.DatabaseURL;

import uk.ac.standrews.cs.nds.rpc.stream.StreamProxy;

/**
 * Test manager that abstracts over the details of instantiating and cleaning up a set of in-memory database instances.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class MemoryTestManager extends TestManager {

    // The names of the in-memory databases.
    private String[] db_names;

    // A factory for making connections to in-memory databases, specialised for a particular set of tests.
    private final IMemoryConnectionDriverFactory connection_driver_factory;

    // -------------------------------------------------------------------------------------------------------

    public MemoryTestManager(final int number_of_databases, final IMemoryConnectionDriverFactory connection_driver_factory) {

        super(number_of_databases);
        this.connection_driver_factory = connection_driver_factory;
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    public void setUp() throws Exception {

        super.setUp();
        StreamProxy.CONNECTION_POOL.setMaxFreeConnectionsPerAddress(0); /* in single process tests connection pooling causes slow tearDown, so it is disabled */
        startupLocator();
        setupDatabaseDescriptorLocation();
        initializeDatabaseProperties();
    }

    @Override
    public void tearDown() throws SQLException {

        closeConnections();
        shutdownLocator();

        super.tearDown();
    }

    @Override
    public ConnectionDriver makeConnectionDriver(final int db_index) {

        return connection_driver_factory.makeConnectionDriver(db_names[db_index], USER_NAME, PASSWORD, connections_to_be_closed);
    }

    // -------------------------------------------------------------------------------------------------------

    @Override
    protected void setUpDatabaseDirectoryPaths() {

        // For in-memory dbs, properties files are stored in the default db directory root.
        // Record this directory so that it can be cleaned up at the end.
        database_base_directory_paths = new String[]{H2O.DEFAULT_DATABASE_DIRECTORY_PATH};
    }

    @Override
    protected boolean failIfPersistentStateCannotBeDeleted() {

        // Can now reliably delete persistent state for in-memory databases.
        return true;
    }

    // -------------------------------------------------------------------------------------------------------

    private void initializeDatabaseProperties() throws IOException {

        db_names = new String[number_of_databases];

        for (int i = 0; i < number_of_databases; i++) {

            db_names[i] = DATABASE_NAME_ROOT + System.currentTimeMillis();
            final DatabaseID url = new DatabaseID(new DatabaseURL("mem", NetUtils.getLocalAddress(), 0, db_names[i], false));

            H2O.initializeDatabaseProperties(url, DIAGNOSTIC_LEVEL, descriptor_file_path, db_names[i], null, null);
        }
    }
}
