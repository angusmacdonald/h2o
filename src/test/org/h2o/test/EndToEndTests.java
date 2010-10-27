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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.junit.Test;

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

    /**
     * Tests whether a new database can be created, data inserted and read back.
     * Preconditions: none
     * Postconditions: persistent database state is not present
     * 
     * @throws SQLException
     */
    @Test
    public void endToEnd() throws SQLException {

        deleteDatabaseState();

        final H2O db = new H2O(DATABASE_NAME, TCP_PORT, WEB_PORT, DATABASE_LOCATION);
        db.startDatabase();
        doQuery();
        db.shutdown();
        db.deleteState();
    }

    /**
     * Tests whether a database can be created, removed, and created again.
     * 
     * @throws SQLException
     */
    @Test
    public void completeCleanUp() throws SQLException {

        endToEnd();
        endToEnd();
    }

    private void deleteDatabaseState() throws SQLException {

        DeleteDbFiles.execute(DATABASE_LOCATION, DATABASE_NAME + TCP_PORT, true);
    }

    private void doQuery() throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + TCP_PORT + "/" + DATABASE_LOCATION + "/" + DATABASE_NAME + TCP_PORT;

        Connection connection = null;
        Statement statement = null;
        try {
            // Create connection to the H2O database instance.
            connection = DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);

            // Create a table and add some data.
            statement = connection.createStatement();

            final int value = 7;
            statement.executeUpdate("CREATE TABLE TEST (ID INT);");
            statement.executeUpdate("INSERT INTO TEST VALUES(" + value + ");");

            // Query the database to check that the data was added successfully.
            final ResultSet result_set = statement.executeQuery("SELECT * FROM TEST;");

            assertTrue(result_set.next());
            assertEquals(value, result_set.getInt(1));
        }
        finally {
            connection.close();
            statement.close();
        }
    }

    private static void obliterateRMIRegistryContents() {

        Registry registry = null;

        try {
            registry = LocateRegistry.getRegistry(CHORD_PORT);
        }
        catch (final RemoteException e) {
            e.printStackTrace();
        }

        if (registry != null) {
            try {
                final String[] listOfObjects = registry.list();

                for (final String l : listOfObjects) {
                    try {
                        System.out.println("found RMI entry: " + l);
                        if (!l.equals("IChordNode")) {
                            System.out.println("unbinding RMI entry: " + l);
                            registry.unbind(l);
                        }
                    }
                    catch (final NotBoundException e) {
                        fail("Failed to remove " + l + " from RMI registry.");
                    }
                }

                assertEquals("Somehow failed to empty RMI registry.", 0, registry.list().length);
            }
            catch (final Exception e) {
                // It happens for tests where the registry was not set up.
            }
        }
    }

    private static final int CHORD_PORT = 40000;
}
