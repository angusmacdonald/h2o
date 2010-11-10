/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
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

import org.h2o.test.H2OTestBase;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.remote_management.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

/**
 * Tests for the PreparedStatement implementation.
 */
public class H2TestPreparedStatement extends H2OTestBase {

    private Connection connection;

    @Override
    protected int getNumberOfDatabases() {

        return 1;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException {

        super.setUp();

        connection = makeTestDriver().getConnection();
    }

    @Test(timeout = 60000)
    public void testLobTempFiles() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
            prepared_statement = connection.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
            for (int i = 0; i < 5; i++) {
                prepared_statement.setInt(1, i);
                if (i % 2 == 0) {
                    prepared_statement.setCharacterStream(2, new StringReader(getString(i)), -1);
                }
                prepared_statement.execute();
            }
            ResultSet rs = statement.executeQuery("SELECT * FROM TEST ORDER BY ID");
            int check = 0;
            for (int i = 0; i < 5; i++) {
                assertTrue(rs.next());
                if (i % 2 == 0) {
                    check = i;
                }
                assertEquals(getString(check), rs.getString(2));
            }
            assertFalse(rs.next());
            statement.execute("DELETE FROM TEST");
            for (int i = 0; i < 3; i++) {
                prepared_statement.setInt(1, i);
                prepared_statement.setCharacterStream(2, new StringReader(getString(i)), -1);
                prepared_statement.addBatch();
            }
            prepared_statement.executeBatch();
            rs = statement.executeQuery("SELECT * FROM TEST ORDER BY ID");
            for (int i = 0; i < 3; i++) {
                assertTrue(rs.next());
                assertEquals(getString(i), rs.getString(2));
            }
            assertFalse(rs.next());
            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }

    private String getString(final int i) {

        return new String(new char[100000]).replace('\0', (char) ('0' + i));
    }

    @Test(timeout = 60000)
    public void testExecuteErrorTwice() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {
            prepared_statement = connection.prepareStatement("CREATE TABLE BAD AS SELECT A");

            try {
                prepared_statement.execute();
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }

            try {
                prepared_statement.execute();
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testTempView() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE TEST(FIELD INT PRIMARY KEY)");
            statement.execute("INSERT INTO TEST VALUES(1)");
            statement.execute("INSERT INTO TEST VALUES(2)");
            prepared_statement = connection.prepareStatement("select FIELD FROM " + "(select FIELD FROM (SELECT FIELD  FROM TEST WHERE FIELD = ?) AS T2 " + "WHERE T2.FIELD = ?) AS T3 WHERE T3.FIELD = ?");
            prepared_statement.setInt(1, 1);
            prepared_statement.setInt(2, 1);
            prepared_statement.setInt(3, 1);
            ResultSet rs = prepared_statement.executeQuery();
            rs.next();
            assertEquals(1, rs.getInt(1));
            prepared_statement.setInt(1, 2);
            prepared_statement.setInt(2, 2);
            prepared_statement.setInt(3, 2);
            rs = prepared_statement.executeQuery();
            rs.next();
            assertEquals(2, rs.getInt(1));
            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(statement);
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testInsertFunction() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();

            ResultSet rs;

            statement.execute("CREATE TABLE TEST(ID INT, H BINARY)");
            prepared_statement = connection.prepareStatement("INSERT INTO TEST VALUES(?, HASH('SHA256', STRINGTOUTF8(?), 5))");
            prepared_statement.setInt(1, 1);
            prepared_statement.setString(2, "One");
            prepared_statement.execute();
            prepared_statement.setInt(1, 2);
            prepared_statement.setString(2, "Two");
            prepared_statement.execute();
            rs = statement.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
            rs.next();
            assertEquals(rs.getInt(1), 2);

            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(statement);
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testMaxRowsChange() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {

            prepared_statement = connection.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 100)");
            ResultSet rs;
            for (int j = 1; j < 20; j++) {
                prepared_statement.setMaxRows(j);
                rs = prepared_statement.executeQuery();
                for (int i = 0; i < j; i++) {
                    assertTrue(rs.next());
                }
                assertFalse(rs.next());
            }
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testUnknownDataType() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {
            try {

                prepared_statement = connection.prepareStatement("SELECT * FROM (SELECT ? FROM DUAL)");
                prepared_statement.setInt(1, 1);
                prepared_statement.execute();
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }

            prepared_statement = connection.prepareStatement("SELECT -?");
            prepared_statement.setInt(1, 1);
            prepared_statement.execute();
            prepared_statement = connection.prepareStatement("SELECT ?-?");
            prepared_statement.setInt(1, 1);
            prepared_statement.setInt(2, 2);
            prepared_statement.execute();
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testCoalesce() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            statement.executeUpdate("create table test(tm timestamp)");
            statement.executeUpdate("insert into test values(current_timestamp)");
            prepared_statement = connection.prepareStatement("update test set tm = coalesce(?,tm)");
            prepared_statement.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
            prepared_statement.executeUpdate();
            statement.executeUpdate("drop table test");
        }
        finally {
            closeIfNotNull(statement);
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testPreparedStatementMetaData() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {
            prepared_statement = connection.prepareStatement("select * from table(x int = ?, name varchar = ?)");
            ResultSetMetaData meta = prepared_statement.getMetaData();
            assertEquals(meta.getColumnCount(), 2);
            assertEquals(meta.getColumnTypeName(1), "INTEGER");
            assertEquals(meta.getColumnTypeName(2), "VARCHAR");
            prepared_statement = connection.prepareStatement("call 1");
            meta = prepared_statement.getMetaData();
            assertEquals(meta.getColumnCount(), 1);
            assertEquals(meta.getColumnTypeName(1), "INTEGER");
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testArray() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {
            prepared_statement = connection.prepareStatement("select * from table(x int = ?) order by x");
            prepared_statement.setObject(1, new Object[]{new BigDecimal("1"), "2"});
            final ResultSet rs = prepared_statement.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), "1");
            rs.next();
            assertEquals(rs.getString(1), "2");
            assertFalse(rs.next());
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testUUIDGeneratedKeys() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE TEST_UUID(id UUID DEFAULT random_UUID() PRIMARY KEY)");
            statement.execute("INSERT INTO TEST_UUID() VALUES()");
            final ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            final byte[] data = rs.getBytes(1);
            assertEquals(data.length, 16);
            statement.execute("DROP TABLE TEST_UUID");
        }
        finally {
            closeIfNotNull(statement);
        }
    }

    @Test(timeout = 60000)
    public void testSetObject() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE TEST(ID INT, DATA BINARY, JAVA OTHER)");
            prepared_statement = connection.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
            prepared_statement.setInt(1, 1);
            prepared_statement.setObject(2, Integer.valueOf(11));
            prepared_statement.setObject(3, null);
            prepared_statement.execute();
            prepared_statement.setInt(1, 2);
            prepared_statement.setObject(2, Integer.valueOf(101), Types.OTHER);
            prepared_statement.setObject(3, Integer.valueOf(103), Types.OTHER);
            prepared_statement.execute();
            final PreparedStatement p2 = connection.prepareStatement("SELECT * FROM TEST ORDER BY ID");
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
            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(statement);
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testDate() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prepared_statement = null;

        try {
            prepared_statement = connection.prepareStatement("SELECT ?");
            final Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
            prepared_statement.setObject(1, new java.util.Date(ts.getTime()));
            final ResultSet rs = prepared_statement.executeQuery();
            rs.next();
            final Timestamp ts2 = rs.getTimestamp(1);
            assertEquals(ts.toString(), ts2.toString());
        }
        finally {
            closeIfNotNull(prepared_statement);
        }
    }

    @Test(timeout = 60000)
    public void testPreparedSubquery() throws SQLException {

        Diagnostic.trace();

        Statement s = null;
        PreparedStatement u = null;
        PreparedStatement p = null;
        try {
            s = connection.createStatement();
            s.executeUpdate("CREATE TABLE TEST(ID IDENTITY, FLAG BIT)");
            s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(0, FALSE)");
            s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(1, FALSE)");
            u = connection.prepareStatement("SELECT ID, FLAG FROM TEST ORDER BY ID");
            p = connection.prepareStatement("UPDATE TEST SET FLAG=true WHERE ID=(SELECT ?)");
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

            p = connection.prepareStatement("SELECT * FROM TEST WHERE EXISTS(SELECT * FROM TEST WHERE ID=?)");
            p.setInt(1, -1);
            rs = p.executeQuery();
            assertFalse(rs.next());
            p.setInt(1, 1);
            rs = p.executeQuery();
            assertTrue(rs.next());

            s.executeUpdate("DROP TABLE IF EXISTS TEST");
        }
        finally {
            closeIfNotNull(s);
            closeIfNotNull(u);
            closeIfNotNull(p);
        }
    }

    @Test(timeout = 60000)
    public void testParameterMetaData() throws SQLException {

        Diagnostic.trace();

        PreparedStatement prep = null;
        PreparedStatement prep3 = null;
        PreparedStatement prep1 = null;
        PreparedStatement prep2 = null;
        Statement stat = null;
        try {
            prep = connection.prepareStatement("SELECT ?, ?, ? FROM DUAL");
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
                // Expected.
            }

            try {
                pm.getPrecision(4);
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }

            prep.close();

            try {
                pm.getPrecision(1);
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }

            stat = connection.createStatement();
            stat.execute("CREATE TABLE TEST3(ID INT, NAME VARCHAR(255), DATA DECIMAL(10,2))");
            prep1 = connection.prepareStatement("UPDATE TEST3 SET ID=?, NAME=?, DATA=?");
            prep2 = connection.prepareStatement("INSERT INTO TEST3 VALUES(?, ?, ?)");
            checkParameter(prep1, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep1, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
            checkParameter(prep1, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            checkParameter(prep2, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep2, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
            checkParameter(prep2, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            prep3 = connection.prepareStatement("SELECT * FROM TEST3 WHERE ID=? AND NAME LIKE ? AND ?>DATA");
            checkParameter(prep3, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
            checkParameter(prep3, 2, "java.lang.String", 12, "VARCHAR", 0, 0);
            checkParameter(prep3, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
            stat.execute("DROP TABLE TEST3");
        }
        finally {
            closeIfNotNull(prep);
            closeIfNotNull(prep1);
            closeIfNotNull(prep2);
            closeIfNotNull(prep3);
            closeIfNotNull(stat);
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

        Diagnostic.trace();

        Statement stat = null;
        PreparedStatement prep = null, prepExe = null;

        try {
            stat = connection.createStatement();
            stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
            stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            stat.execute("INSERT INTO TEST VALUES(2, 'World')");
            stat.execute("create index idxname on test(name);");

            prep = connection.prepareStatement("EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
            assertEquals(prep.getParameterMetaData().getParameterCount(), 1);
            prepExe = connection.prepareStatement("SELECT * FROM TEST WHERE NAME LIKE ?");
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
            closeIfNotNull(stat);
            closeIfNotNull(prep);
            closeIfNotNull(prepExe);
        }
    }

    @Test(timeout = 60000)
    public void testCaseWhen() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;
        try {
            statement = connection.createStatement();

            statement.execute("CREATE TABLE TEST(ID INT)");
            statement.execute("INSERT INTO TEST VALUES(1),(2),(3)");

            ResultSet rs;
            prepared_statement = connection.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
            prepared_statement.setInt(1, 1);
            rs = prepared_statement.executeQuery();
            rs.next();
            String plan = rs.getString(1);
            rs.close();
            prepared_statement = connection.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
            prepared_statement.setInt(1, 1);
            rs = prepared_statement.executeQuery();
            rs.next();
            plan = rs.getString(1);

            prepared_statement = connection.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
            prepared_statement.setInt(1, 1);
            rs = prepared_statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertFalse(rs.next());

            prepared_statement = connection.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
            prepared_statement.setInt(1, 1);
            rs = prepared_statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getInt(1), 1);
            assertFalse(rs.next());

            prepared_statement = connection.prepareStatement("SELECT * FROM TEST WHERE ? IS NULL");
            prepared_statement.setString(1, "Hello");
            rs = prepared_statement.executeQuery();
            assertFalse(rs.next());

            try {
                connection.prepareStatement("select ? from dual union select ? from dual");
                fail();
            }
            catch (final SQLException e) {
                // Expected.
            }

            prepared_statement = connection.prepareStatement("select cast(? as varchar) from dual union select ? from dual");
            assertEquals(prepared_statement.getParameterMetaData().getParameterCount(), 2);
            prepared_statement.setString(1, "a");
            prepared_statement.setString(2, "a");
            rs = prepared_statement.executeQuery();
            rs.next();
            assertEquals(rs.getString(1), "a");
            assertEquals(rs.getString(1), "a");
            assertFalse(rs.next());

            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }

    @Test(timeout = 60000)
    public void testSubquery() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;
        try {
            statement = connection.createStatement();
            statement.execute("CREATE TABLE TEST(ID INT)");
            statement.execute("INSERT INTO TEST VALUES(1),(2),(3)");
            prepared_statement = connection.prepareStatement("select x.id, ? from " + "(select * from test where id in(?, ?)) x where x.id*2 <>  ?");
            assertEquals(prepared_statement.getParameterMetaData().getParameterCount(), 4);
            prepared_statement.setInt(1, 0);
            prepared_statement.setInt(2, 1);
            prepared_statement.setInt(3, 2);
            prepared_statement.setInt(4, 4);
            final ResultSet rs = prepared_statement.executeQuery();
            rs.next();
            assertEquals(rs.getInt(1), 1);
            assertEquals(rs.getInt(2), 0);
            assertFalse(rs.next());
            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }

    @Test(timeout = 60000)
    public void testObject() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prepared_statement = null;

        try {
            statement = connection.createStatement();
            ResultSet rs;
            statement.execute("DROP TABLE IF EXISTS TEST;");
            statement.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
            statement.execute("INSERT INTO TEST VALUES(1, 'Hello')");
            prepared_statement = connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
            prepared_statement.setObject(1, Boolean.valueOf(true));
            prepared_statement.setObject(2, "Abc");
            prepared_statement.setObject(3, new BigDecimal("10.2"));
            prepared_statement.setObject(4, Byte.valueOf((byte) 0xff));
            prepared_statement.setObject(5, new Short(Short.MAX_VALUE));
            prepared_statement.setObject(6, new Integer(Integer.MIN_VALUE));
            prepared_statement.setObject(7, new Long(Long.MAX_VALUE));
            prepared_statement.setObject(8, new Float(Float.MAX_VALUE));
            prepared_statement.setObject(9, new Double(Double.MAX_VALUE));
            prepared_statement.setObject(10, java.sql.Date.valueOf("2001-02-03"));
            prepared_statement.setObject(11, java.sql.Time.valueOf("04:05:06"));
            prepared_statement.setObject(12, java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789"));
            prepared_statement.setObject(13, new java.util.Date(java.sql.Date.valueOf("2001-02-03").getTime()));
            final byte[] arr_original = new byte[]{10, 20, 30};
            prepared_statement.setObject(14, arr_original);
            prepared_statement.setObject(15, Character.valueOf('a'));
            prepared_statement.setObject(16, "2001-01-02", Types.DATE);
            // converting to null seems strange...
            prepared_statement.setObject(17, "2001-01-02", Types.NULL);
            prepared_statement.setObject(18, "3.725", Types.DOUBLE);
            prepared_statement.setObject(19, "23:22:21", Types.TIME);
            prepared_statement.setObject(20, new java.math.BigInteger("12345"), Types.OTHER);
            rs = prepared_statement.executeQuery();
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

            statement.execute("DROP TABLE TEST");
        }
        finally {
            closeIfNotNull(prepared_statement);
            closeIfNotNull(statement);
        }
    }
}
