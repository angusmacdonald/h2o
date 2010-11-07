/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2o.test.H2OTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * Test for big databases.
 */
public class H2TestBigDb extends H2OTestBase {

    private Connection connection;

    @Override
    @Before
    public void setUp() throws SQLException, IOException {

        super.setUp();

        connection = makeConnection();
    }

    @Override
    @After
    public void tearDown() throws SQLException {

        if (connection != null) {
            connection.close();
        }

        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testLeftSummary() throws SQLException {

        Diagnostic.trace();

        final Statement stat = connection.createStatement();
        PreparedStatement prep = null;
        try {
            stat.execute("DROP TABLE IF EXISTS TEST;");
            stat.execute("CREATE TABLE TEST(ID INT, NEG INT AS -ID, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
            stat.execute("CREATE INDEX IDX_NEG ON TEST(NEG, NAME)");
            prep = connection.prepareStatement("INSERT INTO TEST(ID, NAME) VALUES(?, '1234567890')");
            final int len = 1000;
            final int block = 10;
            int left, x = 0;
            for (int i = 0; i < len; i++) {
                left = x + block / 2;

                for (int j = 0; j < block; j++) {
                    prep.setInt(1, x++);
                    prep.execute();
                }
                stat.execute("DELETE FROM TEST WHERE ID>" + left);
                final ResultSet rs = stat.executeQuery("SELECT COUNT(*) FROM TEST");
                rs.next();
                final int count = rs.getInt(1);
            }
        }
        finally {
            prep.close();
            stat.close();
        }
    }

    @Test(timeout = 60000)
    public void testInsert() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;
        PreparedStatement prep = null;
        try {

            stat = connection.createStatement();
            stat.execute("DROP TABLE IF EXISTS TEST;");
            stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            prep = connection.prepareStatement("INSERT INTO TEST(NAME) VALUES('Hello World')");
            final int len = 10000;
            for (int i = 0; i < len; i++) {
                if (i % 1000 == 0) {
                    Thread.yield();
                }
                prep.execute();
            }
        }
        finally {
            prep.close();
            stat.close();
        }
    }
}
