/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test.fixture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.tools.DeleteDbFiles;
import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.h2o.db.manager.PersistentSystemTable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import uk.ac.standrews.cs.nds.registry.IRegistry;
import uk.ac.standrews.cs.nds.registry.stream.RegistryFactory;
import uk.ac.standrews.cs.nds.rpc.stream.StreamProxy;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Base class for JUnit tests. Sets up two databases which are used for the rest of testing.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 * @author Graham Kirby (graham@cs.st-andrews.ac.uk)
 */
public class TestBase2 {

    H2O db1;
    H2O db2;

    H2OLocator locator = null;

    Connection ca = null;
    Connection cb = null;

    protected Statement sa = null;
    protected Statement sb = null;

    private static final int TCP_PORT1 = 9990;
    private static final int TCP_PORT2 = 9999;
    private static final String DATABASE_LOCATION1 = "db_data1";
    private static final String DATABASE_LOCATION2 = "db_data2";
    private static final String DATABASE_NAME = "test";

    private static final String URL_PREFIX = "jdbc:h2:tcp://";
    private static final String DESCRIPTOR_DIRECTORY = "conf";

    @BeforeClass
    public static void initialSetUp() {

        StreamProxy.CONNECTION_POOL.setMaxFreeConnectionsPerAddress(0);
        Diagnostic.setLevel(DiagnosticLevel.FULL);
        Diagnostic.addIgnoredPackage("uk.ac.standrews.cs.stachord");
    }

    @Before
    public void setUp() throws SQLException, IOException, InterruptedException {

        Constants.IS_NON_SM_TEST = true;

        DeleteDbFiles.execute(DATABASE_LOCATION1, DATABASE_NAME, false);
        DeleteDbFiles.execute(DATABASE_LOCATION2, DATABASE_NAME, false);

        locator = new H2OLocator(DATABASE_NAME, 5999, true, DATABASE_LOCATION1);
        final String descriptorFilePath = locator.start();

        db1 = new H2O(DATABASE_NAME, null, DATABASE_LOCATION1, descriptorFilePath, DiagnosticLevel.FULL, 0);
        db2 = new H2O(DATABASE_NAME, null, DATABASE_LOCATION2, descriptorFilePath, DiagnosticLevel.NONE, 0);

        db1.startDatabase();
        db2.startDatabase();

        ca = DriverManager.getConnection(db1.getURL(), PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
        cb = DriverManager.getConnection(db2.getURL(), PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

        sa = ca.createStatement();
        sb = cb.createStatement();

        String sql = "CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));";
        sql += "INSERT INTO TEST VALUES(1, 'Hello');";
        sql += "INSERT INTO TEST VALUES(2, 'World');";

        sa.execute(sql);
    }

    /**
     * @throws SQLException
     * @throws InterruptedException
     */
    @After
    public void tearDown() throws SQLException, InterruptedException {

        Constants.IS_TEAR_DOWN = true;

        try {
            if (sb != null) {
                sb.execute("SHUTDOWN");
            }
            if (sa != null) {
                sa.execute("SHUTDOWN");
            }
        }
        catch (final Exception e1) {

            e1.printStackTrace();
        }

        try {
            if (!sa.isClosed()) {
                sa.close();
            }
        }
        finally {
            try {
                if (!sb.isClosed()) {
                    sb.close();
                }
            }
            finally {
                try {
                    if (!ca.isClosed()) {
                        ca.close();
                    }
                }
                finally {
                    try {
                        if (!cb.isClosed()) {
                            cb.close();
                        }
                    }
                    finally {
                        try {
                            db1.shutdown();
                        }
                        catch (final Exception e) {
                            //It has possibly already been shutdown.
                        }
                        finally {
                            try {
                                db1.deletePersistentState();
                            }
                            finally {
                                try {
                                    db2.shutdown();
                                }
                                catch (final SQLException e) {
                                    //It has possibly already been shutdown.
                                }
                                finally {
                                    try {
                                        db2.deletePersistentState();
                                    }
                                    finally {
                                        locator.shutdown();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        shutdownRegistry();

        Constants.IS_NON_SM_TEST = false;

    }

    public static void shutdownRegistry() {

        try {
            final IRegistry registry = RegistryFactory.FACTORY.getRegistry(InetAddress.getByName(NetUtils.getLocalAddress()));
            registry.shutdown();
        }
        catch (final Exception e) {
            //May already be shutdown.
        }
    }

    /**
     * Utility method which checks that the results of a test query match up to the set of expected values. The 'TEST' class is being used
     * in these tests so the primary keys (int) and names (varchar/string) are required to check the validity of the resultset.
     * 
     * @param key
     *            The set of expected primary keys.
     * @param secondCol
     *            The set of expected names.
     * @param rs
     *            The results actually returned.
     * @throws SQLException
     */
    protected void validateResults(final int[] pKey, final String[] secondCol, final ResultSet rs) throws SQLException {

        assertNotNull("Resultset was null. Probably an incorrectly set test.", rs);

        for (int i = 0; i < pKey.length; i++) {
            if (pKey[i] != 0 && secondCol[i] != null) { // indicates the entry was deleted as part of the test.

                assertTrue(rs.next());
                assertEquals(pKey[i], rs.getInt(1));
                assertEquals(secondCol[i], rs.getString(2));
            }
        }

        assertFalse("Too many entries: " + rs.getInt(1) + ": " + rs.getString(2), rs.next());

        rs.close();
    }
}
