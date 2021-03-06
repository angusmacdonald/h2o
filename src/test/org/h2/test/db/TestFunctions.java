/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Properties;

import org.h2.api.AggregateFunction;
import org.h2.test.TestBase;
import org.h2.tools.SimpleResultSet;
import org.h2.util.IOUtils;

/**
 * Tests for user defined functions and aggregates.
 */
public class TestFunctions extends TestBase implements AggregateFunction {

    static int count;

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
    public void test() throws Exception {

        deleteDb("functions");
        testDeterministic();
        testTransactionId();
        testPrecision();
        testVarArgs();
        testAggregate();
        testFunctions();
        testFileRead();
        deleteDb("functions");
    }

    private void testDeterministic() throws SQLException, IOException {

        final Connection conn = getConnection("functions");
        final Statement stat = conn.createStatement();
        ResultSet rs;

        stat.execute("create alias getCount for \"" + getClass().getName() + ".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(1, rs.getInt(1));
        stat.execute("drop alias getCount");

        stat.execute("create alias getCount deterministic for \"" + getClass().getName() + ".getCount\"");
        setCount(0);
        rs = stat.executeQuery("select getCount() from system_range(1, 2)");
        rs.next();
        assertEquals(0, rs.getInt(1));
        rs.next();
        assertEquals(0, rs.getInt(1));
        stat.execute("drop alias getCount");

        conn.close();
    }

    private void testTransactionId() throws SQLException, IOException {

        if (config.memory) { return; }
        final Connection conn = getConnection("functions");
        final Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        ResultSet rs;
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) == null && rs.wasNull());
        stat.execute("insert into test values(1)");
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) == null && rs.wasNull());
        conn.setAutoCommit(false);
        stat.execute("delete from test");
        rs = stat.executeQuery("call transaction_id()");
        rs.next();
        assertTrue(rs.getString(1) != null);
        stat.execute("drop table test");
        conn.close();
    }

    private void testPrecision() throws SQLException, IOException {

        final Connection conn = getConnection("functions");
        final Statement stat = conn.createStatement();
        stat.execute("create alias no_op for \"" + getClass().getName() + ".noOp\"");
        final PreparedStatement prep = conn.prepareStatement("select * from dual where no_op(1.6)=?");
        prep.setBigDecimal(1, new BigDecimal("1.6"));
        ResultSet rs = prep.executeQuery();
        assertTrue(rs.next());

        stat.execute("create aggregate agg_sum for \"" + getClass().getName() + "\"");
        rs = stat.executeQuery("select agg_sum(1), sum(1.6) from dual");
        rs.next();
        assertEquals(1, rs.getMetaData().getScale(2));
        assertEquals(32767, rs.getMetaData().getScale(1));
        conn.close();
    }

    private void testVarArgs() throws SQLException {

        /*
         * ## Java 1.5 begin ## Connection conn = getConnection("functions"); Statement stat = conn.createStatement();
         * stat.execute("CREATE ALIAS mean FOR \"" + getClass().getName() + ".mean\""); ResultSet rs = stat.executeQuery(
         * "select mean(), mean(10), mean(10, 20), mean(10, 20, 30)"); rs.next(); assertEquals(1.0, rs.getDouble(1)); assertEquals(10.0,
         * rs.getDouble(2)); assertEquals(15.0, rs.getDouble(3)); assertEquals(20.0, rs.getDouble(4));
         * stat.execute("CREATE ALIAS mean2 FOR \"" + getClass().getName() + ".mean2\""); rs = stat.executeQuery(
         * "select mean2(), mean2(10), mean2(10, 20)"); rs.next(); assertEquals(Double.NaN, rs.getDouble(1)); assertEquals(10.0,
         * rs.getDouble(2)); assertEquals(15.0, rs.getDouble(3)); stat.execute("CREATE ALIAS printMean FOR \"" + getClass().getName() +
         * ".printMean\""); rs = stat.executeQuery( "select printMean('A'), printMean('A', 10), " +
         * "printMean('BB', 10, 20), printMean ('CCC', 10, 20, 30)"); rs.next(); assertEquals("A: 0", rs.getString(1));
         * assertEquals("A: 10", rs.getString(2)); assertEquals("BB: 15", rs.getString(3)); assertEquals("CCC: 20", rs.getString(4));
         * conn.close(); ## Java 1.5 end ##
         */
    }

    private void testFileRead() throws Exception {

        final Connection conn = getConnection("functions");
        final Statement stat = conn.createStatement();
        final File f = new File(baseDir + "/test.txt");
        final Properties prop = System.getProperties();
        final FileOutputStream out = new FileOutputStream(f);
        prop.store(out, "");
        out.close();
        ResultSet rs = stat.executeQuery("SELECT LENGTH(FILE_READ('" + baseDir + "/test.txt')) LEN");
        rs.next();
        assertEquals(f.length(), rs.getInt(1));
        rs = stat.executeQuery("SELECT FILE_READ('" + baseDir + "/test.txt') PROP");
        rs.next();
        final Properties p2 = new Properties();
        p2.load(rs.getBinaryStream(1));
        assertEquals(prop.size(), p2.size());
        rs = stat.executeQuery("SELECT FILE_READ('" + baseDir + "/test.txt', NULL) PROP");
        rs.next();
        final String ps = rs.getString(1);
        final FileReader r = new FileReader(f);
        final String ps2 = IOUtils.readStringAndClose(r, -1);
        assertEquals(ps, ps2);
        f.delete();
        conn.close();
    }

    /**
     * This median implementation keeps all objects in memory.
     */
    public static class MedianString implements AggregateFunction {

        private final ArrayList list = new ArrayList();

        @Override
        public void add(final Object value) {

            list.add(value.toString());
        }

        @Override
        public Object getResult() {

            return list.get(list.size() / 2);
        }

        @Override
        public int getType(final int[] inputType) {

            return Types.VARCHAR;
        }

        @Override
        public void init(final Connection conn) {

            // nothing to do
        }

    }

    private void testAggregate() throws SQLException, IOException {

        deleteDb("functions");
        Connection conn = getConnection("functions");
        Statement stat = conn.createStatement();
        stat.execute("CREATE AGGREGATE MEDIAN FOR \"" + MedianString.class.getName() + "\"");
        stat.execute("CREATE AGGREGATE IF NOT EXISTS MEDIAN FOR \"" + MedianString.class.getName() + "\"");
        ResultSet rs = stat.executeQuery("SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
        rs.next();
        assertEquals("5", rs.getString(1));
        conn.close();

        if (config.memory) { return; }

        conn = getConnection("functions");
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT MEDIAN(X) FROM SYSTEM_RANGE(1, 9)");
        final DatabaseMetaData meta = conn.getMetaData();
        rs = meta.getProcedures(null, null, "MEDIAN");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs = stat.executeQuery("SCRIPT");
        boolean found = false;
        while (rs.next()) {
            final String sql = rs.getString(1);
            if (sql.indexOf("MEDIAN") >= 0) {
                found = true;
            }
        }
        assertTrue(found);
        stat.execute("DROP AGGREGATE MEDIAN");
        stat.execute("DROP AGGREGATE IF EXISTS MEDIAN");
        conn.close();
    }

    private void testFunctions() throws SQLException, IOException {

        deleteDb("functions");
        final Connection conn = getConnection("functions");
        final Statement stat = conn.createStatement();
        test(stat, "abs(null)", null);
        test(stat, "abs(1)", "1");
        test(stat, "abs(1)", "1");

        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE ALIAS ADD_ROW FOR \"" + getClass().getName() + ".addRow\"");
        ResultSet rs;
        rs = stat.executeQuery("CALL ADD_ROW(1, 'Hello')");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL ADD_ROW(2, 'World')");

        stat.execute("CREATE ALIAS SELECT_F FOR \"" + getClass().getName() + ".select\"");
        rs = stat.executeQuery("CALL SELECT_F('SELECT * FROM TEST ORDER BY ID')");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT NAME FROM SELECT_F('SELECT * FROM TEST ORDER BY NAME') ORDER BY NAME DESC");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals(rs.getString(1), "World");
        rs.next();
        assertEquals(rs.getString(1), "Hello");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST WHERE ID=' || ID) FROM TEST ORDER BY ID");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals("((1, Hello))", rs.getString(1));
        rs.next();
        assertEquals("((2, World))", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT SELECT_F('SELECT * FROM TEST ORDER BY ID') FROM DUAL");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals("((1, Hello), (2, World))", rs.getString(1));
        assertFalse(rs.next());

        try {
            rs = stat.executeQuery("CALL SELECT_F('ERROR')");
            fail();
        }
        catch (final SQLException e) {
            assertEquals("42001", e.getSQLState());
        }

        stat.execute("CREATE ALIAS SIMPLE FOR \"" + getClass().getName() + ".simpleResultSet\"");
        rs = stat.executeQuery("CALL SIMPLE(2, 1,1,1,1,1,1,1)");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "World");
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM SIMPLE(1, 1,1,1,1,1,1,1)");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ARRAY FOR \"" + getClass().getName() + ".getArray\"");
        rs = stat.executeQuery("CALL ARRAY()");
        assertEquals(rs.getMetaData().getColumnCount(), 2);
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS ROOT FOR \"" + getClass().getName() + ".root\"");
        rs = stat.executeQuery("CALL ROOT(9)");
        rs.next();
        assertEquals(rs.getInt(1), 3);
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS MAX_ID FOR \"" + getClass().getName() + ".selectMaxId\"");
        rs = stat.executeQuery("CALL MAX_ID()");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT * FROM MAX_ID()");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());

        rs = stat.executeQuery("CALL CASE WHEN -9 < 0 THEN 0 ELSE ROOT(-9) END");
        rs.next();
        assertEquals(rs.getInt(1), 0);
        assertFalse(rs.next());

        stat.execute("CREATE ALIAS blob2stream FOR \"" + getClass().getName() + ".blob2stream\"");
        stat.execute("CREATE ALIAS stream2stream FOR \"" + getClass().getName() + ".stream2stream\"");
        stat.execute("CREATE TABLE TEST_BLOB(ID INT PRIMARY KEY, VALUE BLOB)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(0, null)");
        stat.execute("INSERT INTO TEST_BLOB VALUES(1, 'edd1f011edd1f011edd1f011')");
        rs = stat.executeQuery("SELECT blob2stream(VALUE) FROM TEST_BLOB");
        while (rs.next()) {
            // ignore
        }
        rs.close();
        rs = stat.executeQuery("SELECT stream2stream(VALUE) FROM TEST_BLOB");
        while (rs.next()) {
            // ignore
        }

        stat.execute("CREATE ALIAS NULL_RESULT FOR \"" + getClass().getName() + ".nullResultSet\"");
        rs = stat.executeQuery("CALL NULL_RESULT()");
        assertEquals(rs.getMetaData().getColumnCount(), 1);
        rs.next();
        assertEquals(rs.getString(1), null);
        assertFalse(rs.next());

        conn.close();
    }

    private void test(final Statement stat, final String sql, final String value) throws SQLException {

        final ResultSet rs = stat.executeQuery("CALL " + sql);
        rs.next();
        final String s = rs.getString(1);
        assertEquals(value, s);
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param value
     *            the blob
     * @return the input stream
     */
    public static BufferedInputStream blob2stream(final Blob value) throws SQLException {

        if (value == null) { return null; }
        final BufferedInputStream bufferedInStream = new BufferedInputStream(value.getBinaryStream());
        return bufferedInStream;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param value
     *            the input stream
     * @return the buffered input stream
     */
    public static BufferedInputStream stream2stream(final InputStream value) {

        if (value == null) { return null; }
        final BufferedInputStream bufferedInStream = new BufferedInputStream(value);
        return bufferedInStream;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param conn
     *            the connection
     * @param id
     *            the test id
     * @param name
     *            the text
     * @return the count
     */
    public static int addRow(final Connection conn, final int id, final String name) throws SQLException {

        conn.createStatement().execute("INSERT INTO TEST VALUES(" + id + ", '" + name + "')");
        final ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM TEST");
        rs.next();
        final int result = rs.getInt(1);
        rs.close();
        return result;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param conn
     *            the connection
     * @param sql
     *            the SQL statement
     * @return the result set
     */
    public static ResultSet select(final Connection conn, final String sql) throws SQLException {

        final Statement stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return stat.executeQuery(sql);
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param conn
     *            the connection
     * @return the result set
     */
    public static ResultSet selectMaxId(final Connection conn) throws SQLException {

        return conn.createStatement().executeQuery("SELECT MAX(ID) FROM TEST");
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @return the test array
     */
    public static Object[] getArray() {

        return new Object[]{new Integer(0), "Hello"};
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param conn
     *            the connection
     * @return the result set
     */
    public static ResultSet nullResultSet(final Connection conn) throws SQLException {

        final PreparedStatement statement = conn.prepareStatement("select null from system_range(1,1)");
        return statement.executeQuery();
    }

    /**
     * Test method to create a simple result set.
     * 
     * @param count
     *            the number of rows
     * @param ip
     *            an int
     * @param bp
     *            a boolean
     * @param fp
     *            a float
     * @param dp
     *            a double
     * @param lp
     *            a long
     * @param byParam
     *            a byte
     * @param sp
     *            a short
     * @return a result set
     */
    public static ResultSet simpleResultSet(final Integer count, final int ip, final boolean bp, final float fp, final double dp, final long lp, final byte byParam, final short sp) throws SQLException {

        final SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("ID", Types.INTEGER, 10, 0);
        rs.addColumn("NAME", Types.VARCHAR, 255, 0);
        if (count == null) {
            if (ip != 0 || bp || fp != 0.0 || dp != 0.0 || sp != 0 || lp != 0 || byParam != 0) { throw new Error("params not 0/false"); }
        }
        if (count != null) {
            if (ip != 1 || !bp || fp != 1.0 || dp != 1.0 || sp != 1 || lp != 1 || byParam != 1) { throw new Error("params not 1/true"); }
            if (count.intValue() >= 1) {
                rs.addRow(new Object[]{new Integer(0), "Hello"});
            }
            if (count.intValue() >= 2) {
                rs.addRow(new Object[]{new Integer(1), "World"});
            }
        }
        return rs;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param value
     *            the value
     * @return the square root
     */
    public static int root(final int value) {

        if (value < 0) {
            TestBase.logError("function called but should not", null);
        }
        return (int) Math.sqrt(value);
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @return 1
     */
    public static double mean() {

        return 1;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param dec
     *            the value
     * @return the value
     */
    public static BigDecimal noOp(final BigDecimal dec) {

        return dec;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @return the count
     */
    public static int getCount() {

        return count++;
    }

    private static void setCount(final int newCount) {

        count = newCount;
    }

    /**
     * This method is called via reflection from the database.
     * 
     * @param values
     *            the values
     * @return the mean value
     */
    /*
     * ## Java 1.5 begin ## public static double mean(double... values) { double sum = 0; for (double x : values) { sum += x; } return sum /
     * values.length; } ## Java 1.5 end ##
     */

    /**
     * This method is called via reflection from the database.
     * 
     * @param conn
     *            the connection
     * @param values
     *            the values
     * @return the mean value
     */
    /*
     * ## Java 1.5 begin ## public static double mean2(Connection conn, double... values) { conn.getClass(); double sum = 0; for (double x :
     * values) { sum += x; } return sum / values.length; } ## Java 1.5 end ##
     */

    /**
     * This method is called via reflection from the database.
     * 
     * @param prefix
     *            the print prefix
     * @param values
     *            the values
     * @return the text
     */
    /*
     * ## Java 1.5 begin ## public static String printMean(String prefix, double... values) { double sum = 0; for (double x : values) { sum
     * += x; } return prefix + ": " + (int) (sum / values.length); } ## Java 1.5 end ##
     */

    public void add(final Object value) throws SQLException {

    }

    public Object getResult() throws SQLException {

        return new BigDecimal("1.6");
    }

    public int getType(final int[] inputTypes) throws SQLException {

        if (inputTypes.length != 1 || inputTypes[0] != Types.INTEGER) { throw new SQLException("unexpected data type"); }
        return Types.DECIMAL;
    }

    public void init(final Connection conn) throws SQLException {

    }

}
