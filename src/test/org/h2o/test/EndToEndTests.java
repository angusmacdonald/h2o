/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.junit.Test;

/**
 * User-centric tests.
 *
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class EndToEndTests {

    // The name of the database domain.
    private static final String DATABASE_NAME = "MyFirstDatabase";

    // Where the database will be created (where persisted state is stored).
    private static final String DATABASE_LOCATION = "db_data";

    // The port on which the database's TCP JDBC server will run.
    private static final int TCP_PORT = 9999;

    // The port on which the database's web interface will run.
    private static final int WEB_PORT = 8282;

    private static final String USER_NAME = "sa";
    private static final String PASSWORD = "";

    private static final int value = 7;

    /**
     * Tests whether a new database can be created, data inserted and read back.
     * Preconditions: none
     * Postconditions: persistent database state is not present
     * 
     * @throws SQLException if the test fails
     */
    @Test
    public void simpleLifeCycle() throws SQLException {

        deleteDatabaseState();

        final H2O db = initDB();
        db.startDatabase();
        doLifeCycle();
        db.shutdown();

        db.deleteState();
    }

    /**
     * Tests whether a database can be created, removed, and created again.
     * 
     * @throws SQLException if the test fails
     */
    @Test
    public void completeCleanUp() throws SQLException {

        simpleLifeCycle();
        simpleLifeCycle();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another.
     * @throws SQLException if the test fails
     */
    @Test
    public void persistence() throws SQLException {

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();
        doCreateAndInsert(true);
        db.shutdown();

        db = initDB();
        db.startDatabase();
        doCheckValues();
        db.shutdown();

        db.deleteState();
    }

    /**
     * Tests whether data can be inserted during one instantiation of a database and read in another.
     * @throws SQLException if the test fails
     */
    @Test
    public void noAutoCommit() throws SQLException {

        deleteDatabaseState();

        H2O db = initDB();
        db.startDatabase();
        doCreateAndInsert(false);
        db.shutdown();

        db = initDB();
        db.startDatabase();
        doCheckNoValues();
        db.shutdown();

        db.deleteState();
    }

    // -------------------------------------------------------------------------------------------------------

    interface IDBAction {

        void execute(Connection connection) throws SQLException;
    }

    private H2O initDB() {

        return new H2O(DATABASE_NAME, TCP_PORT, WEB_PORT, DATABASE_LOCATION);
    }

    private void performAction(final IDBAction action) throws SQLException {

        Connection connection = null;
        try {
            connection = makeConnection();
            action.execute(connection);
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void doLifeCycle() throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                createTable(connection);
                insertValues(connection);
                checkValues(connection);
            }
        });
    }

    private void doCreateAndInsert(final boolean auto_commit) throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                connection.setAutoCommit(auto_commit);
                createTable(connection);
                insertValues(connection);
            }
        });
    }

    private void doCheckValues() throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                checkValues(connection);
            }
        });
    }

    private void doCheckNoValues() throws SQLException {

        performAction(new IDBAction() {

            @Override
            public void execute(final Connection connection) throws SQLException {

                checkNoValues(connection);
            }
        });
    }

    private void deleteDatabaseState() throws SQLException {

        DeleteDbFiles.execute(DATABASE_LOCATION, DATABASE_NAME + TCP_PORT, true);
    }

    private void createTable(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Create a table.
            statement.executeUpdate("CREATE TABLE TEST (ID INT);");
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void insertValues(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Add some data.
            statement.executeUpdate("INSERT INTO TEST VALUES(" + value + ");");
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void checkValues(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Query the database to check that the data was added successfully.
            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            assertTrue(result_set.next());
            assertEquals(value, result_set.getInt(1));
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void checkNoValues(final Connection connection) throws SQLException {

        Statement statement = null;
        try {
            statement = connection.createStatement();

            // Query the database to check that the data was added successfully.
            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            assertFalse(result_set.next());
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    private void closeIfNotNull(final Statement statement) throws SQLException {

        if (statement != null) {
            statement.close();
        }

    }

    private Connection makeConnection() throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + TCP_PORT + "/" + DATABASE_LOCATION + "/" + DATABASE_NAME + TCP_PORT;

        // Create connection to the H2O database instance.
        return DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);
    }
}
