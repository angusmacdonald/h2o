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

import org.h2.util.MemoryUtils;
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
    public void testLargeTable() throws SQLException {

        Diagnostic.trace();

        final Statement stat = connection.createStatement();
        PreparedStatement prep = null;
        try {
            stat.execute("CREATE CACHED TABLE TEST(" + "M_CODE CHAR(1) DEFAULT CAST(RAND()*9 AS INT)," + "PRD_CODE CHAR(20) DEFAULT SECURE_RAND(10)," + "ORG_CODE_SUPPLIER CHAR(13) DEFAULT SECURE_RAND(6)," + "PRD_CODE_1 CHAR(14) DEFAULT SECURE_RAND(7),"
                            + "PRD_CODE_2 CHAR(20)  DEFAULT SECURE_RAND(10)," + "ORG_CODE CHAR(13)  DEFAULT SECURE_RAND(6)," + "SUBSTITUTED_BY CHAR(20) DEFAULT SECURE_RAND(10)," + "SUBSTITUTED_BY_2 CHAR(14) DEFAULT SECURE_RAND(7)," + "SUBSTITUTION_FOR CHAR(20) DEFAULT SECURE_RAND(10),"
                            + "SUBSTITUTION_FOR_2 CHAR(14) DEFAULT SECURE_RAND(7)," + "TEST CHAR(2) DEFAULT SECURE_RAND(1)," + "TEST_2 CHAR(2) DEFAULT SECURE_RAND(1)," + "TEST_3 DECIMAL(7,2) DEFAULT RAND()," + "PRIMARY_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1),"
                            + "RATE_PRICE_ORDER_UNIT DECIMAL(9,3) DEFAULT RAND()," + "ORDER_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "ORDER_QTY_MIN DECIMAL(6,1) DEFAULT RAND()," + "ORDER_QTY_LOT_SIZE DECIMAL(6,1) DEFAULT RAND()," + "ORDER_UNIT_CODE_2 CHAR(3) DEFAULT SECURE_RAND(1),"
                            + "PRICE_GROUP CHAR(20) DEFAULT SECURE_RAND(10)," + "LEAD_TIME INTEGER DEFAULT RAND()," + "LEAD_TIME_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "PRD_GROUP CHAR(10) DEFAULT SECURE_RAND(5)," + "WEIGHT_GROSS DECIMAL(7,3) DEFAULT RAND(),"
                            + "WEIGHT_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "PACK_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "PACK_LENGTH DECIMAL(7,3) DEFAULT RAND()," + "PACK_WIDTH DECIMAL(7,3) DEFAULT RAND()," + "PACK_HEIGHT DECIMAL(7,3) DEFAULT RAND(),"
                            + "SIZE_UNIT_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "STATUS_CODE CHAR(3) DEFAULT SECURE_RAND(1)," + "INTRA_STAT_CODE CHAR(12) DEFAULT SECURE_RAND(6)," + "PRD_TITLE CHAR(50) DEFAULT SECURE_RAND(25)," + "VALID_FROM DATE DEFAULT NOW()," + "MOD_DATUM DATE DEFAULT NOW())");
            final int len = 50000;

            prep = connection.prepareStatement("INSERT INTO TEST(PRD_CODE) VALUES('abc' || ?)");
            long time = System.currentTimeMillis();
            for (int i = 0; i < len; i++) {
                if (i % 1000 == 0) {
                    final long t = System.currentTimeMillis();
                    if (t - time > 1000) {
                        time = t;
                        final int free = MemoryUtils.getMemoryFree();
                    }
                }
                prep.setInt(1, i);
                prep.execute();
            }
            stat.execute("CREATE INDEX IDX_TEST_PRD_CODE ON TEST(PRD_CODE)");
            final ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
            final int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 0; i < columns; i++) {
                    rs.getString(i + 1);
                }
            }
        }
        finally {

            stat.close();
            prep.close();
        }
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
