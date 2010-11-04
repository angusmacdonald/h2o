/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Tests for the PreparedStatement implementation.
 */
public class H2TestPreparedStatementFailing extends H2TestBase {

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
    public void testBlob() throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        trace("testBlob");
        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = conn.createStatement();
            ResultSet rs;
            stat.execute("CREATE TABLE T_BLOB(ID INT PRIMARY KEY,V1 BLOB,V2 BLOB)");
            trace("table created");
            prep = conn.prepareStatement("INSERT INTO T_BLOB VALUES(?,?,?)");

            prep.setInt(1, 1);
            prep.setBytes(2, null);
            prep.setNull(3, Types.BINARY);
            prep.executeUpdate();

            prep.setInt(1, 2);
            prep.setBinaryStream(2, null, 0);
            prep.setNull(3, Types.BLOB);
            prep.executeUpdate();

            final int length = getLength();
            final byte[] big1 = new byte[length];
            final byte[] big2 = new byte[length];
            for (int i = 0; i < big1.length; i++) {
                big1[i] = (byte) (i * 11 % 254);
                big2[i] = (byte) (i * 17 % 251);
            }

            prep.setInt(1, 3);
            prep.setBytes(2, big1);
            prep.setBytes(3, big2);
            prep.executeUpdate();

            prep.setInt(1, 4);
            ByteArrayInputStream buffer;
            buffer = new ByteArrayInputStream(big2);
            prep.setBinaryStream(2, buffer, big2.length);
            buffer = new ByteArrayInputStream(big1);
            prep.setBinaryStream(3, buffer, big1.length);
            prep.executeUpdate();
            try {
                buffer.close();
                trace("buffer not closed");
            }
            catch (final IOException e) {
                trace("buffer closed");
            }

            prep.setInt(1, 5);
            buffer = new ByteArrayInputStream(big2);
            prep.setObject(2, buffer, Types.BLOB, 0);
            buffer = new ByteArrayInputStream(big1);
            prep.setObject(3, buffer);
            prep.executeUpdate();

            rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_BLOB ORDER BY ID");

            rs.next();
            assertEquals(rs.getInt(1), 1);
            assertTrue(rs.getBytes(2) == null && rs.wasNull());
            assertTrue(rs.getBytes(3) == null && rs.wasNull());

            rs.next();
            assertEquals(rs.getInt(1), 2);
            assertTrue(rs.getBytes(2) == null && rs.wasNull());
            assertTrue(rs.getBytes(3) == null && rs.wasNull());

            rs.next();
            assertEquals(rs.getInt(1), 3);
            checkBytes(big1, rs.getBytes(2));
            checkBytes(big2, rs.getBytes(3));

            rs.next();

            assertEquals(rs.getInt(1), 4);
            checkBytes(big2, rs.getBytes(2));
            checkBytes(big1, rs.getBytes(3));

            rs.next();
            assertEquals(rs.getInt(1), 5);
            checkBytes(big2, rs.getBytes(2));
            checkBytes(big1, rs.getBytes(3));

            assertFalse(rs.next());
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
    public void testCancelReuse() throws Exception {

        Diagnostic.trace(DiagnosticLevel.FULL);

        Statement createAlias = null;

        PreparedStatement prep = null;

        try {

            createAlias = conn.createStatement();
            createAlias.execute("CREATE ALIAS YIELD FOR \"java.lang.Thread.yield\"");

            prep = conn.prepareStatement("SELECT YIELD() FROM SYSTEM_RANGE(1, 1000000) LIMIT ?");
            prep.setInt(1, 100000000);

            final PreparedStatement finalPrep = prep;
            final Thread t = new Thread() {

                @Override
                public void run() {

                    try {
                        finalPrep.execute();
                    }
                    catch (final SQLException e) {
                        // ignore
                    }
                }
            };
            t.start();
            Thread.sleep(10);
            try {
                prep.cancel();
            }
            catch (final SQLException e) {
                this.assertKnownException(e);
            }
            prep.setInt(1, 1);
            final ResultSet rs = prep.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 0);
            assertFalse(rs.next());
        }
        finally {
            if (createAlias != null) {
                createAlias.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testClob() throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        trace("testClob");
        Statement stat = null;
        PreparedStatement prep = null;

        try {
            stat = conn.createStatement();

            ResultSet rs;
            stat.execute("CREATE TABLE T_CLOB(ID INT PRIMARY KEY,V1 CLOB,V2 CLOB)");
            final StringBuilder asciiBuffer = new StringBuilder();
            final int len = getLength();
            for (int i = 0; i < len; i++) {
                asciiBuffer.append((char) ('a' + i % 20));
            }
            final String ascii1 = asciiBuffer.toString();
            final String ascii2 = "Number2 " + ascii1;
            prep = conn.prepareStatement("INSERT INTO T_CLOB VALUES(?,?,?)");

            prep.setInt(1, 1);
            prep.setString(2, null);
            prep.setNull(3, Types.CLOB);
            prep.executeUpdate();

            prep.clearParameters();
            prep.setInt(1, 2);
            prep.setAsciiStream(2, null, 0);
            prep.setCharacterStream(3, null, 0);
            prep.executeUpdate();

            prep.clearParameters();
            prep.setInt(1, 3);
            prep.setCharacterStream(2, new StringReader(ascii1), ascii1.length());
            prep.setCharacterStream(3, null, 0);
            prep.setAsciiStream(3, new ByteArrayInputStream(ascii2.getBytes()), ascii2.length());
            prep.executeUpdate();

            prep.clearParameters();
            prep.setInt(1, 4);
            prep.setNull(2, Types.CLOB);
            prep.setString(2, ascii2);
            prep.setCharacterStream(3, null, 0);
            prep.setNull(3, Types.CLOB);
            prep.setString(3, ascii1);
            prep.executeUpdate();

            prep.clearParameters();
            prep.setInt(1, 5);
            prep.setObject(2, new StringReader(ascii1));
            prep.setObject(3, new StringReader(ascii2), Types.CLOB, 0);
            prep.executeUpdate();

            rs = stat.executeQuery("SELECT ID, V1, V2 FROM T_CLOB ORDER BY ID");

            rs.next();
            assertEquals(rs.getInt(1), 1);
            assertTrue(rs.getCharacterStream(2) == null && rs.wasNull());
            assertTrue(rs.getAsciiStream(3) == null && rs.wasNull());

            rs.next();
            assertEquals(rs.getInt(1), 2);
            assertTrue(rs.getString(2) == null && rs.wasNull());
            assertTrue(rs.getString(3) == null && rs.wasNull());

            rs.next();
            assertEquals(rs.getInt(1), 3);
            assertEquals(rs.getString(2), ascii1);
            assertEquals(rs.getString(3), ascii2);

            rs.next();
            assertEquals(rs.getInt(1), 4);
            assertEquals(rs.getString(2), ascii2);
            assertEquals(rs.getString(3), ascii1);

            rs.next();
            assertEquals(rs.getInt(1), 5);
            assertEquals(rs.getString(2), ascii1);
            assertEquals(rs.getString(3), ascii2);

            assertFalse(rs.next());
            assertEquals(null, prep.getWarnings());
            prep.clearWarnings();
            assertEquals(null, prep.getWarnings());
            assertEquals(conn, prep.getConnection());
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
    public void testDataTypes() throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        Statement stat = null;
        PreparedStatement prep = null;
        try {
            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            stat = conn.createStatement();

            ResultSet rs;
            trace("Create tables");
            stat.execute("CREATE TABLE T_INT(ID INT PRIMARY KEY,VALUE INT)");
            stat.execute("CREATE TABLE T_VARCHAR(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
            stat.execute("CREATE TABLE T_DECIMAL_0(ID INT PRIMARY KEY,VALUE DECIMAL(30,0))");
            stat.execute("CREATE TABLE T_DECIMAL_10(ID INT PRIMARY KEY,VALUE DECIMAL(20,10))");
            stat.execute("CREATE TABLE T_DATETIME(ID INT PRIMARY KEY,VALUE DATETIME)");
            prep = conn.prepareStatement("INSERT INTO T_INT VALUES(?,?)", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            prep.setInt(1, 1);
            prep.setInt(2, 0);
            prep.executeUpdate();
            prep.setInt(1, 2);
            prep.setInt(2, -1);
            prep.executeUpdate();
            prep.setInt(1, 3);
            prep.setInt(2, 3);
            prep.executeUpdate();
            prep.setInt(1, 4);
            prep.setNull(2, Types.INTEGER);
            prep.executeUpdate();
            prep.setInt(1, 5);
            prep.setBigDecimal(2, new java.math.BigDecimal("0"));
            prep.executeUpdate();
            prep.setInt(1, 6);
            prep.setString(2, "-1");
            prep.executeUpdate();
            prep.setInt(1, 7);
            prep.setObject(2, Integer.valueOf(3));
            prep.executeUpdate();
            prep.setObject(1, "8");
            // should throw an exception
            prep.setObject(2, null);
            // some databases don't allow calling setObject with null (no data type)
            prep.executeUpdate();
            prep.setInt(1, 9);
            prep.setObject(2, Integer.valueOf(-4), Types.VARCHAR);
            prep.executeUpdate();
            prep.setInt(1, 10);
            prep.setObject(2, "5", Types.INTEGER);
            prep.executeUpdate();
            prep.setInt(1, 11);
            prep.setObject(2, null, Types.INTEGER);
            prep.executeUpdate();
            prep.setInt(1, 12);
            prep.setBoolean(2, true);
            prep.executeUpdate();
            prep.setInt(1, 13);
            prep.setBoolean(2, false);
            prep.executeUpdate();
            prep.setInt(1, 14);
            prep.setByte(2, (byte) -20);
            prep.executeUpdate();
            prep.setInt(1, 15);
            prep.setByte(2, (byte) 100);
            prep.executeUpdate();
            prep.setInt(1, 16);
            prep.setShort(2, (short) 30000);
            prep.executeUpdate();
            prep.setInt(1, 17);
            prep.setShort(2, (short) -30000);
            prep.executeUpdate();
            prep.setInt(1, 18);
            prep.setLong(2, Integer.MAX_VALUE);
            prep.executeUpdate();
            prep.setInt(1, 19);
            prep.setLong(2, Integer.MIN_VALUE);
            prep.executeUpdate();

            assertTrue(stat.execute("SELECT * FROM T_INT ORDER BY ID"));
            rs = stat.getResultSet();
            assertResultSetOrdered(rs, new String[][]{{"1", "0"}, {"2", "-1"}, {"3", "3"}, {"4", null}, {"5", "0"}, {"6", "-1"}, {"7", "3"}, {"8", null}, {"9", "-4"}, {"10", "5"}, {"11", null}, {"12", "1"}, {"13", "0"}, {"14", "-20"}, {"15", "100"}, {"16", "30000"}, {"17", "-30000"},
                            {"18", "" + Integer.MAX_VALUE}, {"19", "" + Integer.MIN_VALUE},});

            prep = conn.prepareStatement("INSERT INTO T_DECIMAL_0 VALUES(?,?)");
            prep.setInt(1, 1);
            prep.setLong(2, Long.MAX_VALUE);
            prep.executeUpdate();
            prep.setInt(1, 2);
            prep.setLong(2, Long.MIN_VALUE);
            prep.executeUpdate();
            prep.setInt(1, 3);
            prep.setFloat(2, 10);
            prep.executeUpdate();
            prep.setInt(1, 4);
            prep.setFloat(2, -20);
            prep.executeUpdate();
            prep.setInt(1, 5);
            prep.setFloat(2, 30);
            prep.executeUpdate();
            prep.setInt(1, 6);
            prep.setFloat(2, -40);
            prep.executeUpdate();

            rs = stat.executeQuery("SELECT VALUE FROM T_DECIMAL_0 ORDER BY ID");
            checkBigDecimal(rs, new String[]{"" + Long.MAX_VALUE, "" + Long.MIN_VALUE, "10", "-20", "30", "-40"});

            // getMoreResults
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("INSERT INTO TEST VALUES(1)");
            prep = conn.prepareStatement("SELECT * FROM TEST");
            // just to check if it doesn't throw an exception - it may be null
            prep.getMetaData();
            assertTrue(prep.execute());
            rs = prep.getResultSet();
            assertFalse(prep.getMoreResults());
            try {
                // supposed to be closed now
                rs.next();
                fail("getMoreResults didn't close this result set");
            }
            catch (final SQLException e) {
                trace("no error - getMoreResults is supposed to close the result set");
            }
            assertEquals(-1, prep.getUpdateCount());
            prep = conn.prepareStatement("DELETE FROM TEST");
            prep.executeUpdate();
            assertFalse(prep.getMoreResults());
            assertEquals(-1, prep.getUpdateCount());
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
    public void testPrepareRecompile() throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        Statement stat = null;
        PreparedStatement prep = null;
        ResultSet rs;

        try {
            stat = conn.createStatement();
            prep = conn.prepareStatement("SELECT COUNT(*) FROM DUAL WHERE ? IS NULL");
            prep.setString(1, null);
            prep.executeQuery();
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("DROP TABLE TEST");
            prep.setString(1, null);
            prep.executeQuery();
            prep.setString(1, "X");
            rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getInt(1), 0);

            stat.execute("CREATE TABLE t1 (c1 INT, c2 VARCHAR(10))");
            stat.execute("INSERT INTO t1 SELECT X, CONCAT('Test', X)  FROM SYSTEM_RANGE(1, 5);");
            prep = conn.prepareStatement("SELECT c1, c2 FROM t1 WHERE c1 = ?");
            prep.setInt(1, 1);
            prep.executeQuery();
            stat.execute("CREATE TABLE t2 (x int PRIMARY KEY)");
            prep.setInt(1, 2);
            rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getInt(1), 2);
            prep.setInt(1, 3);
            rs = prep.executeQuery();
            rs.next();
            assertEquals(rs.getInt(1), 3);
            stat.execute("DROP TABLE t1, t2");
        }
        finally {
            stat.close();
            prep.close();
        }
    }

    private void checkBigDecimal(final ResultSet rs, final String[] value) throws SQLException {

        for (final String v : value) {
            assertTrue(rs.next());
            final java.math.BigDecimal x = rs.getBigDecimal(1);
            trace("v=" + v + " x=" + x);
            if (v == null) {
                assertEquals(null, x);
            }
            else {
                assertEquals(0, x.compareTo(new java.math.BigDecimal(v)));
            }
        }
        assertFalse(rs.next());
    }

    private int getLength() throws SQLException {

        return getSize(LOB_SIZE, LOB_SIZE_BIG);
    }

    private void checkBytes(final byte[] big1, final byte[] arr) {

        for (int i = 0; i < arr.length; i++) {
            assertEquals(arr[i], big1[i]);
        }
    }
}
