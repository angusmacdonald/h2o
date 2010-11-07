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
public class H2TestBigDbLongRunning extends H2OTestBase {

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

    @Test(timeout = 600000)
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
            for (int i = 0; i < len; i++) {
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
}
