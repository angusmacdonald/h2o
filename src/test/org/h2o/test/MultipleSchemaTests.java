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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2o.test.fixture.TestBase2;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * A small set of tests that check whether the system copes with multiple different schemas (containing tables of the same name).
 * 
 * <p>
 * Tests include basic queries and testing the ability to replicate schema's.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class MultipleSchemaTests extends TestBase2 {

    /**
     * Tests that a table in a non-default schema is added successfully to the System Table.
     * @throws SQLException 
     */
    @Test
    public void testSystemTableAdd() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        setup();

        sa.execute("DROP TABLE IF EXISTS SCHEMA2.TEST");
        sa.execute("DROP SCHEMA IF EXISTS SCHEMA2");
    }

    /**
     * Tests that a table in a non-default schema is dropped successfully from the System Table.
     * @throws SQLException 
     */
    @Test
    public void testSystemTableDrop() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        setup();

        // Drop the table and check the result of the update.
        sa.execute("DROP TABLE SCHEMA2.TEST");

        assertEquals(0, sa.getUpdateCount());

        // Check that the System Table has correct information.
        sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");
        final ResultSet rs = sa.getResultSet();

        if (rs.next()) {
            assertEquals("TEST", rs.getString(1));
            assertEquals("PUBLIC", rs.getString(2));
        }

        assertFalse("There should only be one entry here.", rs.next());
    }

    /**
     * Tests that a non-default schema is dropped successfully from the System Table.
     * @throws SQLException 
     */
    @Test
    public void testSystemTableDropSchema() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        setup();

        // Drop the table and check the result of the update.

        sa.execute("DROP TABLE IF EXISTS SCHEMA2.TEST");
        sa.execute("DROP SCHEMA SCHEMA2");

        assertEquals(0, sa.getUpdateCount());

        // Now check that the System Table has correct information.
        sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");
        final ResultSet rs = sa.getResultSet();

        if (rs.next()) {
            assertEquals("TEST", rs.getString(1));
            assertEquals("PUBLIC", rs.getString(2));
        }

        assertFalse("There should only be one entry here.", rs.next());
    }

    private void setup() throws SQLException {

        sa.execute("CREATE SCHEMA SCHEMA2");
        sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");

        sa.execute("SELECT tablename, schemaname FROM H2O.H2O_TABLE;");

        ResultSet rs = null;
        try {
            rs = sa.getResultSet();

            assertTrue(rs.next());
            assertEquals("TEST", rs.getString(1));
            assertEquals("PUBLIC", rs.getString(2));

            assertTrue("Expected a System Table entry here.", rs.next());
            assertEquals("TEST", rs.getString(1));
            assertEquals("SCHEMA2", rs.getString(2));
        }
        finally {
            if (rs != null) {
                rs.close();
            }
        }
    }

    /**
     * Tests the feature to create multiple replicas at the same time.
     * @throws SQLException 
     */
    @Test
    public void createMultipleTestTablesLocal() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sa.execute("CREATE SCHEMA SCHEMA2");
        sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        sa.execute("INSERT INTO SCHEMA2.TEST VALUES(4, 'Meh');");
        sa.execute("INSERT INTO SCHEMA2.TEST VALUES(5, 'Heh');");

        sa.execute("SELECT LOCAL ONLY * FROM SCHEMA2.TEST ORDER BY ID;");

        final int[] pKey = {4, 5};
        final String[] secondCol = {"Meh", "Heh"};

        validateResults(pKey, secondCol, sa.getResultSet());

        sa.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

        final int[] pKey2 = {1, 2};
        final String[] secondCol2 = {"Hello", "World"};

        validateResults(pKey2, secondCol2, sa.getResultSet());
    }

    /**
     * Creates a new TEST table in a different schema, then creates remote replicas for each. Tested for success by accessing these remote
     * replicas.
     * @throws SQLException 
     */
    @Test
    public void createMultipleTestReplicas() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sa.execute("CREATE SCHEMA SCHEMA2");
        sa.execute("CREATE TABLE SCHEMA2.TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        sa.execute("INSERT INTO SCHEMA2.TEST VALUES(4, 'Meh');");
        sa.execute("INSERT INTO SCHEMA2.TEST VALUES(5, 'Heh');");

        sb.execute("CREATE REPLICA SCHEMA2.TEST;");

        assertEquals(0, sb.getUpdateCount());

        sb.execute("SELECT LOCAL ONLY * FROM SCHEMA2.TEST ORDER BY ID;");

        final int[] pKey = {4, 5};
        final String[] secondCol = {"Meh", "Heh"};

        validateResults(pKey, secondCol, sb.getResultSet());

        sb.execute("SELECT LOCAL ONLY * FROM TEST ORDER BY ID;");

        final int[] pKey2 = {1, 2};
        final String[] secondCol2 = {"Hello", "World"};

        validateResults(pKey2, secondCol2, sb.getResultSet());
    }

    /**
     * Tries to access SCHEMA2.TEST, where the SCHEMA2 schema does not exist. This should return an error, rather than finding the TEST
     * table in the default schema.
     * @throws SQLException 
     */
    @Test(expected = SQLException.class)
    public void testTableDoesntExist() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sa.execute("SELECT * FROM SCHEMA2.TEST");

        assertFalse("Expected a failure", sa.getUpdateCount() == 0);
    }

    /**
     * Tries to create a replica for SCHEMA2.TEST, where the SCHEMA2 schema does not exist. This should return an error, rather than finding
     * the TEST table in the default schema.
     * @throws SQLException 
     */
    @Test(expected = SQLException.class)
    public void testTableDoesntExistForReplica() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sb.execute("CREATE REPLICA SCHEMA2.TEST;");

        assertFalse("Expected a failure", sb.getUpdateCount() == 0);
    }

    /**
     * Tries to create a replica of a schema that doesn't exist. Should fail.
     * @throws SQLException 
     */
    @Test(expected = SQLException.class)
    public void testTableDoesntExistForReplica2() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sb.execute("CREATE REPLICA SCHEMA2;");

        assertFalse("Expected a failure", sb.getUpdateCount() == 0);
    }

    /**
     * Tests replication of an entire schema of tables.
     * @throws SQLException 
     */
    @Test
    public void replicateSchema() throws SQLException {

        Diagnostic.traceNoEvent(DiagnosticLevel.INIT, "STARTING TEST");

        sa.execute("CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));");
        sa.execute("INSERT INTO TEST2 VALUES(4, 'Meh');");
        sa.execute("INSERT INTO TEST2 VALUES(5, 'Heh');");

        sb.execute("CREATE REPLICA SCHEMA PUBLIC;");

        assertEquals("Expected update count to be '0'", 0, sb.getUpdateCount());

        /*
         * Check that the local copy has only two entries.
         */
        sb.execute("SELECT LOCAL ONLY * FROM TEST2 ORDER BY ID;");

        final int[] pKey = {4, 5};
        final String[] secondCol = {"Meh", "Heh"};

        validateResults(pKey, secondCol, sb.getResultSet());
    }
}
