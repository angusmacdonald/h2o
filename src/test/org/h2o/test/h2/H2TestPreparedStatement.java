/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;

import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for the PreparedStatement implementation.
 */
public class H2TestPreparedStatement extends H2TestBase {

    private static final int LOB_SIZE = 4000, LOB_SIZE_BIG = 512 * 1024;

    private LocatorServer ls;

    private Connection conn;

    @Before
    public void setUp() throws SQLException {

        DeleteDbFiles.execute("data\\test\\", "preparedStatement", true);

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        config = new TestAll();

        conn = getConnection("preparedStatement");
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {

        conn.close();

        ls.setRunning(false);
        while (!ls.isFinished()) {
            Thread.sleep(SHUTDOWN_CHECK_DELAY);
        };
    }

    @Test(timeout = 60000)
    public void testLobTempFiles() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
            prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
            for (int i = 0; i < 5; i++) {
                prep.setInt(1, i);
                if (i % 2 == 0) {
                    prep.setCharacterStream(2, new StringReader(getString(i)), -1);
                }
                prep.execute();
            }
            ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
            int check = 0;
            for (int i = 0; i < 5; i++) {
                assertTrue(rs.next());
                if (i % 2 == 0) {
                    check = i;
                }
                assertEquals(getString(check), rs.getString(2));
            }
            assertFalse(rs.next());
            stat.execute("DELETE FROM TEST");
            for (int i = 0; i < 3; i++) {
                prep.setInt(1, i);
                prep.setCharacterStream(2, new StringReader(getString(i)), -1);
                prep.addBatch();
            }
            prep.executeBatch();
            rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
            for (int i = 0; i < 3; i++) {
                assertTrue(rs.next());
                assertEquals(getString(i), rs.getString(2));
            }
            assertFalse(rs.next());
            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (prep != null) {
                prep.close();
            }
            if (stat != null) {
                stat.close();
            }
        }
    }

    private String getString(final int i) {

        return new String(new char[100000]).replace('\0', (char) ('0' + i));
    }

    @Test(timeout = 60000)
    public void testExecuteErrorTwice() throws SQLException {

        PreparedStatement prep = null;

        try {
            prep = conn.prepareStatement("CREATE TABLE BAD AS SELECT A");
            try {
                prep.execute();
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
            try {
                prep.execute();
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testTempView() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;
        try {

            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(FIELD INT PRIMARY KEY)");
            stat.execute("INSERT INTO TEST VALUES(1)");
            stat.execute("INSERT INTO TEST VALUES(2)");
            prep = conn.prepareStatement("select FIELD FROM " + "(select FIELD FROM (SELECT FIELD  FROM TEST WHERE FIELD = ?) AS T2 " + "WHERE T2.FIELD = ?) AS T3 WHERE T3.FIELD = ?");
            prep.setInt(1, 1);
            prep.setInt(2, 1);
            prep.setInt(3, 1);
            ResultSet rs = prep.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
            prep.setInt(1, 2);
            prep.setInt(2, 2);
            prep.setInt(3, 2);
            rs = prep.executeQuery();
            rs.next();
            assertEquals(2, rs.getInt(1));
            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testInsertFunction() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;
        try {
            stat = conn.createStatement();

            ResultSet rs;

            stat.execute("CREATE TABLE TEST(ID INT, H BINARY)");
            prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, HASH('SHA256', STRINGTOUTF8(?), 5))");
            prep.setInt(1, 1);
            prep.setString(2, "One");
            prep.execute();
            prep.setInt(1, 2);
            prep.setString(2, "Two");
            prep.execute();
            rs = stat.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
            rs.next();
            assertEquals(rs.getInt(1), 2);

            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testMaxRowsChange() throws SQLException {

        PreparedStatement prep = null;

        try {

            prep = conn.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 100)");
            ResultSet rs;
            for (int j = 1; j < 20; j++) {
                prep.setMaxRows(j);
                rs = prep.executeQuery();
                for (int i = 0; i < j; i++) {
                    assertTrue(rs.next());
                }
                assertFalse(rs.next());
            }
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testUnknownDataType() throws SQLException {

        PreparedStatement prep = null;

        try {
            try {

                prep = conn.prepareStatement("SELECT * FROM (SELECT ? FROM DUAL)");
                prep.setInt(1, 1);
                prep.execute();
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
            prep = conn.prepareStatement("SELECT -?");
            prep.setInt(1, 1);
            prep.execute();
            prep = conn.prepareStatement("SELECT ?-?");
            prep.setInt(1, 1);
            prep.setInt(2, 2);
            prep.execute();
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testCoalesce() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = conn.createStatement();
            stat.executeUpdate("create table test(tm timestamp)");
            stat.executeUpdate("insert into test values(current_timestamp)");
            prep = conn.prepareStatement("update test set tm = coalesce(?,tm)");
            prep.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
            prep.executeUpdate();
            stat.executeUpdate("drop table test");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testPreparedStatementMetaData() throws SQLException {

        PreparedStatement prep = null;

        try {
            prep = conn.prepareStatement("select * from table(x int = ?, name varchar = ?)");
            ResultSetMetaData meta = prep.getMetaData();
            assertEquals(meta.getColumnCount(), 2);
            assertEquals(meta.getColumnTypeName(1), "INTEGER");
            assertEquals(meta.getColumnTypeName(2), "VARCHAR");
            prep = conn.prepareStatement("call 1");
            meta = prep.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnTypeName(1), "INTEGER");
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testArray() throws SQLException {

        PreparedStatement prep = null;

        try {
            prep = conn.prepareStatement("select * from table(x int = ?) order by x");
            prep.setObject(1, new Object[]{new BigDecimal("1"), "2"});
            final ResultSet rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), "1");
            rs.next();
            assertEquals(rs.getString(1), "2");
            assertFalse(rs.next());
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testUUIDGeneratedKeys() throws SQLException {

        Statement stat = null;

        try {
            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST_UUID(id UUID DEFAULT random_UUID() PRIMARY KEY)");
            stat.execute("INSERT INTO TEST_UUID() VALUES()");
            final ResultSet rs = stat.getGeneratedKeys();
            rs.next();
            final byte[] data = rs.getBytes(1);
            assertEquals(data.length, 16);
            stat.execute("DROP TABLE TEST_UUID");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testSetObject() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;
        try {
            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT, DATA BINARY, JAVA OTHER)");
            prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
            prep.setInt(1, 1);
            prep.setObject(2, Integer.valueOf(11));
            prep.setObject(3, null);
            prep.execute();
            prep.setInt(1, 2);
            prep.setObject(2, Integer.valueOf(101), Types.OTHER);
            prep.setObject(3, Integer.valueOf(103), Types.OTHER);
            prep.execute();
            final PreparedStatement p2 = conn.prepareStatement("SELECT * FROM TEST ORDER BY ID");
            final ResultSet rs = p2.executeQuery();
            rs.next();
            Object o = rs.getObject(2);
            assertTrue(o instanceof byte[]);
            assertTrue(rs.getObject(3) == null);
            rs.next();
            o = rs.getObject(2);
            assertTrue(o instanceof byte[]);
            o = rs.getObject(3);
            assertTrue(o instanceof Integer);
            assertEquals(((Integer) o).intValue(), 103);
            assertFalse(rs.next());
            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testDate() throws SQLException {

        PreparedStatement prep = null;

        try {
            prep = conn.prepareStatement("SELECT ?");
            final Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
            prep.setObject(1, new java.util.Date(ts.getTime()));
            final ResultSet rs = prep.executeQuery();
            rs.next();
            final Timestamp ts2 = rs.getTimestamp(1);
            assertEquals(ts.toString(), ts2.toString());
        }
        finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testPreparedSubquery() throws SQLException {

        Statement s = null;
        PreparedStatement u = null;
        PreparedStatement p = null;
        try {
            s = conn.createStatement();
            s.executeUpdate("CREATE TABLE TEST(ID IDENTITY, FLAG BIT)");
            s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(0, FALSE)");
            s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(1, FALSE)");
            u = conn.prepareStatement("SELECT ID, FLAG FROM TEST ORDER BY ID");
            p = conn.prepareStatement("UPDATE TEST SET FLAG=true WHERE ID=(SELECT ?)");
            p.clearParameters();
            p.setLong(1, 0);
            assertEquals(p.executeUpdate(), 1);
            p.clearParameters();
            p.setLong(1, 1);
            assertEquals(p.executeUpdate(), 1);
            ResultSet rs = u.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 0);
            assertTrue(rs.getBoolean(2));
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertTrue(rs.getBoolean(2));

            p = conn.prepareStatement("SELECT * FROM TEST WHERE EXISTS(SELECT * FROM TEST WHERE ID=?)");
            p.setInt(1, -1);
            rs = p.executeQuery();
            assertFalse(rs.next());
            p.setInt(1, 1);
            rs = p.executeQuery();
            assertTrue(rs.next());

            s.executeUpdate("DROP TABLE IF EXISTS TEST");
        }
        finally {
            s.close();
            u.close();
            p.close();
        }
    }

    @Test(timeout = 60000)
    public void testParameterMetaData() throws SQLException {

        PreparedStatement prep = null;
        PreparedStatement prep3 = null;
        PreparedStatement prep1 = null;
        PreparedStatement prep2 = null;
        Statement stat = null;
        try {
            prep = conn.prepareStatement("SELECT ?, ?, ? FROM DUAL");
            final ParameterMetaData pm = prep.getParameterMetaData();
            assertEquals(pm.getParameterClassName(1), "java.lang.String");
            assertEquals(pm.getParameterTypeName(1), "VARCHAR");
            assertEquals(pm.getParameterCount(), 3);
            assertEquals(pm.getParameterMode(1), ParameterMetaData.parameterModeIn);
            assertEquals(pm.getParameterType(1), Types.VARCHAR);
            assertEquals(pm.getPrecision(1), 0);
            assertEquals(pm.getScale(1), 0);
            assertEquals(pm.isNullable(1), ResultSetMetaData.columnNullableUnknown);
            assertEquals(pm.isSigned(1), true);
            try {
                pm.getPrecision(0);
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
            try {
                pm.getPrecision(4);
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
            prep.close();
            try {
                pm.getPrecision(1);
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }

            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST3(ID INT, NAME VARCHAR(255), DATA DECIMAL(10,2))");
            prep1 = conn.prepareStatement("UPDATE TEST3 SET ID=?, NAME=?, DATA=?");
            prep2 = conn.prepareStatement("INSERT INTO TEST3 VALUES(?, ?, ?)");
            checkParameter(prep1, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep1, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
            checkParameter(prep1, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            checkParameter(prep2, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep2, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
            checkParameter(prep2, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            prep3 = conn.prepareStatement("SELECT * FROM TEST3 WHERE ID=? AND NAME LIKE ? AND ?>DATA");
            checkParameter(prep3, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep3, 2, "java.lang.String", 12, "VARCHAR", 0, 0);
            checkParameter(prep3, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            stat.execute("DROP TABLE TEST3");
        }
        finally {
            if (prep != null) {
                prep.close();
            }
            prep1.close();
            prep2.close();
            prep3.close();
            if (stat != null) {
                stat.close();
            }
        }
    }

    private void checkParameter(final PreparedStatement prep, final int index, final String className, final int type, final String typeName, final int precision, final int scale) throws SQLException {

        final ParameterMetaData meta = prep.getParameterMetaData();
        assertEquals(className, meta.getParameterClassName(index));
        assertEquals(type, meta.getParameterType(index));
        assertEquals(typeName, meta.getParameterTypeName(index));
        assertEquals(precision, meta.getPrecision(index));
        assertEquals(scale, meta.getScale(index));
    }

    @Test(timeout = 60000)
    public void testLikeIndex() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null, prepExe = null;

        try {
            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            stat.execute("INSERT INTO TEST VALUES(2, 'World')");
            stat.execute("create index idxname on test(name);");

            prep = conn.prepareStatement("EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
            assertEquals(prep.getParameterMetaData().getParameterCount(), 1);
            prepExe = conn.prepareStatement("SELECT * FROM TEST WHERE NAME LIKE ?");
            prep.setString(1, "%orld");
            prepExe.setString(1, "%orld");
            ResultSet rs = prep.executeQuery();
            rs.next();
            final String plan = rs.getString(1);
            assertTrue(plan.indexOf("TABLE_SCAN") >= 0);
            rs = prepExe.executeQuery();
            rs.next();
            assertEquals(rs.getString(2), "World");
            assertFalse(rs.next());

            prep.setString(1, "H%");
            prepExe.setString(1, "H%");
            rs = prep.executeQuery();
            rs.next();
            final String plan1 = rs.getString(1);
            assertTrue(plan1.indexOf("IDXNAME") >= 0);
            rs = prepExe.executeQuery();
            rs.next();
            assertEquals(rs.getString(2), "Hello");
            assertFalse(rs.next());

            stat.execute("DROP TABLE IF EXISTS TEST");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
            prepExe.close();
        }
    }

    @Test(timeout = 60000)
    public void testCasewhen() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;
        try {
            stat = conn.createStatement();

            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");

            ResultSet rs;
            prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
            prep.setInt(1, 1);
            rs = prep.executeQuery();
            rs.next();
            String plan = rs.getString(1);
            trace(plan);
            rs.close();
            prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
            prep.setInt(1, 1);
            rs = prep.executeQuery();
            rs.next();
            plan = rs.getString(1);
            trace(plan);

            prep = conn.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
            prep.setInt(1, 1);
            rs = prep.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertFalse(rs.next());

            prep = conn.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
            prep.setInt(1, 1);
            rs = prep.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertFalse(rs.next());

            prep = conn.prepareStatement("SELECT * FROM TEST WHERE ? IS NULL");
            prep.setString(1, "Hello");
            rs = prep.executeQuery();
            assertFalse(rs.next());
            try {
                conn.prepareStatement("select ? from dual union select ? from dual");
                fail();
            }
            catch (final SQLException e) {
                assertKnownException(e);
            }
            prep = conn.prepareStatement("select cast(? as varchar) from dual union select ? from dual");
            assertEquals(prep.getParameterMetaData().getParameterCount(), 2);
            prep.setString(1, "a");
            prep.setString(2, "a");
            rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), "a");
            assertEquals(rs.getString(1), "a");
            assertFalse(rs.next());

            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (prep != null) {
                prep.close();
            }
            if (stat != null) {
                stat.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testSubquery() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;
        try {
            stat = conn.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
            prep = conn.prepareStatement("select x.id, ? from " + "(select * from test where id in(?, ?)) x where x.id*2 <>  ?");
            assertEquals(prep.getParameterMetaData().getParameterCount(), 4);
            prep.setInt(1, 0);
            prep.setInt(2, 1);
            prep.setInt(3, 2);
            prep.setInt(4, 4);
            final ResultSet rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 0);
            assertFalse(rs.next());
            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (stat != null) {
                stat.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testObject() throws SQLException {

        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = conn.createStatement();
            ResultSet rs;
            stat.execute("DROP TABLE IF EXISTS TEST;");
            stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            prep = conn.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
            prep.setObject(1, Boolean.valueOf(true));
            prep.setObject(2, "Abc");
            prep.setObject(3, new BigDecimal("10.2"));
            prep.setObject(4, Byte.valueOf((byte) 0xff));
            prep.setObject(5, new Short(Short.MAX_VALUE));
            prep.setObject(6, new Integer(Integer.MIN_VALUE));
            prep.setObject(7, new Long(Long.MAX_VALUE));
            prep.setObject(8, new Float(Float.MAX_VALUE));
            prep.setObject(9, new Double(Double.MAX_VALUE));
            prep.setObject(10, java.sql.Date.valueOf("2001-02-03"));
            prep.setObject(11, java.sql.Time.valueOf("04:05:06"));
            prep.setObject(12, java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789"));
            prep.setObject(13, new java.util.Date(java.sql.Date.valueOf("2001-02-03").getTime()));
            final byte[] arr_original = new byte[]{10, 20, 30};
            prep.setObject(14, arr_original);
            prep.setObject(15, Character.valueOf('a'));
            prep.setObject(16, "2001-01-02", Types.DATE);
            // converting to null seems strange...
            prep.setObject(17, "2001-01-02", Types.NULL);
            prep.setObject(18, "3.725", Types.DOUBLE);
            prep.setObject(19, "23:22:21", Types.TIME);
            prep.setObject(20, new java.math.BigInteger("12345"), Types.OTHER);
            rs = prep.executeQuery();
            rs.next();
            assertTrue(rs.getObject(1).equals(Boolean.valueOf(true)));
            assertTrue(rs.getObject(2).equals("Abc"));
            assertTrue(rs.getObject(3).equals(new BigDecimal("10.2")));
            assertTrue(rs.getObject(4).equals(Byte.valueOf((byte) 0xff)));
            assertTrue(rs.getObject(5).equals(new Short(Short.MAX_VALUE)));
            assertTrue(rs.getObject(6).equals(new Integer(Integer.MIN_VALUE)));
            assertTrue(rs.getObject(7).equals(new Long(Long.MAX_VALUE)));
            assertTrue(rs.getObject(8).equals(new Float(Float.MAX_VALUE)));
            assertTrue(rs.getObject(9).equals(new Double(Double.MAX_VALUE)));
            assertTrue(rs.getObject(10).equals(java.sql.Date.valueOf("2001-02-03")));
            assertEquals(rs.getObject(11).toString(), "04:05:06");
            assertTrue(rs.getObject(11).equals(java.sql.Time.valueOf("04:05:06")));
            assertTrue(rs.getObject(12).equals(java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789")));
            assertTrue(rs.getObject(13).equals(java.sql.Timestamp.valueOf("2001-02-03 00:00:00")));
            final byte[] arr = (byte[]) rs.getObject(14);
            assertEquals(arr[0], arr_original[0]);
            assertEquals(arr[1], arr_original[1]);
            assertEquals(arr[2], arr_original[2]);
            assertTrue(rs.getObject(15).equals(Character.valueOf('a')));
            assertTrue(rs.getObject(16).equals(java.sql.Date.valueOf("2001-01-02")));
            assertTrue(rs.getObject(17) == null && rs.wasNull());
            assertTrue(rs.getObject(18).equals(new Double(3.725)));
            assertTrue(rs.getObject(19).equals(java.sql.Time.valueOf("23:22:21")));
            assertTrue(rs.getObject(20).equals(new java.math.BigInteger("12345")));

            stat.execute("DROP TABLE TEST");
        }
        finally {
            if (prep != null) {
                prep.close();
            }
            if (stat != null) {
                stat.close();
            }
        }
    }
}
