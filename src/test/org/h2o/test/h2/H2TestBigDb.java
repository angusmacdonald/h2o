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
    protected int getNumberOfDatabases() {

        return 1;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException {

        super.setUp();

        connection = getConnections()[0];
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

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("DROP TABLE IF EXISTS TEST;");
            statement.execute("CREATE TABLE TEST(ID INT, NEG INT AS -ID, NAME VARCHAR, PRIMARY KEY(ID, NAME))");
            statement.execute("CREATE INDEX IDX_NEG ON TEST(NEG, NAME)");
            prepared_statement = connection.prepareStatement("INSERT INTO TEST(ID, NAME) VALUES(?, '1234567890')");
            final int len = 1000;
            final int block = 10;
            int left, x = 0;
            for (int i = 0; i < len; i++) {
                left = x + block / 2;

                for (int j = 0; j < block; j++) {
                    prepared_statement.setInt(1, x++);
                    prepared_statement.execute();
                }
                statement.execute("DELETE FROM TEST WHERE ID>" + left);
                final ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM TEST");
                rs.next();
                final int count = rs.getInt(1);
            }
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }

    @Test(timeout = 60000)
    public void testInsert() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;
        try {

            statement = connection.createStatement();
            statement.execute("DROP TABLE IF EXISTS TEST;");
            statement.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
            prepared_statement = connection.prepareStatement("INSERT INTO TEST(NAME) VALUES('Hello World')");
            final int len = 10000;
            for (int i = 0; i < len; i++) {
                if (i % 1000 == 0) {
                    Thread.yield();
                }
                prepared_statement.execute();
            }
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }
}
