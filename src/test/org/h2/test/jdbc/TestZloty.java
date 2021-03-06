/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.test.TestBase;

/**
 * Tests a custom BigDecimal implementation, as well as direct modification of a byte in a byte array.
 */
public class TestZloty extends TestBase {

    /**
     * Run just this test.
     * 
     * @param a
     *            ignored
     */
    public static void main(final String[] a) throws Exception {

        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException, IOException {

        testZloty();
        testModifyBytes();
        deleteDb("zloty");
    }

    /**
     * This class overrides BigDecimal and implements some strange comparison method.
     */
    private static class ZlotyBigDecimal extends BigDecimal {

        private static final long serialVersionUID = -8004563653683501484L;

        public ZlotyBigDecimal(final String s) {

            super(s);
        }

        @Override
        public int compareTo(final BigDecimal bd) {

            return -super.compareTo(bd);
        }

    }

    private void testModifyBytes() throws SQLException, IOException {

        deleteDb("zloty");
        final Connection conn = getConnection("zloty");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, DATA BINARY)");
        final PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        final byte[] shared = new byte[1];
        prep.setInt(1, 0);
        prep.setBytes(2, shared);
        prep.execute();
        shared[0] = 1;
        prep.setInt(1, 1);
        prep.setBytes(2, shared);
        prep.execute();
        final ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getBytes(2)[0], 0);
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getBytes(2)[0], 1);
        rs.getBytes(2)[0] = 2;
        assertEquals(rs.getBytes(2)[0], 1);
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * H2 destroyer application ;->
     * 
     * @author Maciej Wegorkiewicz
     * @throws IOException 
     */
    private void testZloty() throws SQLException, IOException {

        deleteDb("zloty");
        final Connection conn = getConnection("zloty");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, AMOUNT DECIMAL)");
        final PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setBigDecimal(2, new BigDecimal("10.0"));
        prep.execute();
        prep.setInt(1, 2);
        try {
            prep.setBigDecimal(2, new ZlotyBigDecimal("11.0"));
            prep.execute();
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }

        prep.setInt(1, 3);
        try {
            final BigDecimal value = new BigDecimal("12.100000") {

                private static final long serialVersionUID = -7909023971521750844L;

                @Override
                public String toString() {

                    return "12,100000 EURO";
                }
            };
            prep.setBigDecimal(2, value);
            prep.execute();
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }

        conn.close();
    }

}
