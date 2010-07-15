/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.h2o.h2;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
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

import org.h2.h2o.util.locator.LocatorServer;
import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
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
	public void setUp() throws SQLException{
		DeleteDbFiles.execute("data\\test\\", "preparedStatement", true);

		ls = new LocatorServer(29999, "junitLocator");
		ls.createNewLocatorFile();
		ls.start();

		config = new TestAll();


		conn = getConnection("preparedStatement");
	}

	@After
	public void tearDown() throws SQLException{
		conn.close();

		ls.setRunning(false);
		while (!ls.isFinished()){};

	}


	@Test
	public void testLobTempFiles() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
		PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
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

	private String getString(int i) {
		return new String(new char[100000]).replace('\0', (char) ('0' + i));
	}


	@Test
	public void testExecuteErrorTwice() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("CREATE TABLE BAD AS SELECT A");
		try {
			prep.execute();
			fail();
		} catch (SQLException e) {
			assertKnownException(e);
		}
		try {
			prep.execute();
			fail();
		} catch (SQLException e) {
			assertKnownException(e);
		}
	}


	@Test
	public void testTempView() throws SQLException {
		Statement stat = conn.createStatement();
		PreparedStatement prep;
		stat.execute("CREATE TABLE TEST(FIELD INT PRIMARY KEY)");
		stat.execute("INSERT INTO TEST VALUES(1)");
		stat.execute("INSERT INTO TEST VALUES(2)");
		prep = conn.prepareStatement("select FIELD FROM "
				+ "(select FIELD FROM (SELECT FIELD  FROM TEST WHERE FIELD = ?) AS T2 "
				+ "WHERE T2.FIELD = ?) AS T3 WHERE T3.FIELD = ?");
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

	@Test
	public void testInsertFunction() throws SQLException {
		Statement stat = conn.createStatement();
		PreparedStatement prep;
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

	@Test
	public void testPrepareRecompile() throws SQLException {
		Statement stat = conn.createStatement();
		PreparedStatement prep;
		ResultSet rs;

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

	@Test
	public void testMaxRowsChange() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT * FROM SYSTEM_RANGE(1, 100)");
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

	@Test
	public void testUnknownDataType() throws SQLException {
		try {
			PreparedStatement prep = conn.prepareStatement(
			"SELECT * FROM (SELECT ? FROM DUAL)");
			prep.setInt(1, 1);
			prep.execute();
			fail();
		} catch (SQLException e) {
			assertKnownException(e);
		}
		PreparedStatement prep = conn.prepareStatement("SELECT -?");
		prep.setInt(1, 1);
		prep.execute();
		prep = conn.prepareStatement("SELECT ?-?");
		prep.setInt(1, 1);
		prep.setInt(2, 2);
		prep.execute();
	}

	@Test
	public void testCancelReuse() throws Exception {
		conn.createStatement().execute("CREATE ALIAS YIELD FOR \"java.lang.Thread.yield\"");
		final PreparedStatement prep = conn.prepareStatement("SELECT YIELD() FROM SYSTEM_RANGE(1, 1000000) LIMIT ?");
		prep.setInt(1, 100000000);
		Thread t = new Thread() {
			public void run() {
				try {
					prep.execute();
				} catch (SQLException e) {
					// ignore
				}
			}
		};
		t.start();
		Thread.sleep(10);
		try {
			prep.cancel();
		} catch (SQLException e) {
			this.assertKnownException(e);
		}
		prep.setInt(1, 1);
		ResultSet rs = prep.executeQuery();
		assertTrue(rs.next());
		assertEquals(rs.getInt(1), 0);
		assertFalse(rs.next());
	}

	@Test
	public void testCoalesce() throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate("create table test(tm timestamp)");
		stat.executeUpdate("insert into test values(current_timestamp)");
		PreparedStatement prep = conn.prepareStatement("update test set tm = coalesce(?,tm)");
		prep.setTimestamp(1, new java.sql.Timestamp(System.currentTimeMillis()));
		prep.executeUpdate();
		stat.executeUpdate("drop table test");
	}

	@Test
	public void testPreparedStatementMetaData() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("select * from table(x int = ?, name varchar = ?)");
		ResultSetMetaData meta = prep.getMetaData();
		assertEquals(meta.getColumnCount(), 2);
		assertEquals(meta.getColumnTypeName(1), "INTEGER");
		assertEquals(meta.getColumnTypeName(2), "VARCHAR");
		prep = conn.prepareStatement("call 1");
		meta = prep.getMetaData();
		assertEquals(meta.getColumnCount(), 1);
		assertEquals(meta.getColumnTypeName(1), "INTEGER");
	}

	@Test
	public void testArray() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("select * from table(x int = ?) order by x");
		prep.setObject(1, new Object[] { new BigDecimal("1"), "2" });
		ResultSet rs = prep.executeQuery();
		rs.next();
		assertEquals(rs.getString(1), "1");
		rs.next();
		assertEquals(rs.getString(1), "2");
		assertFalse(rs.next());
	}

	@Test
	public void testUUIDGeneratedKeys() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST_UUID(id UUID DEFAULT random_UUID() PRIMARY KEY)");
		stat.execute("INSERT INTO TEST_UUID() VALUES()");
		ResultSet rs = stat.getGeneratedKeys();
		rs.next();
		byte[] data = rs.getBytes(1);
		assertEquals(data.length, 16);
		stat.execute("DROP TABLE TEST_UUID");
	}

	@Test
	public void testSetObject() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST(ID INT, DATA BINARY, JAVA OTHER)");
		PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
		prep.setInt(1, 1);
		prep.setObject(2, new Integer(11));
		prep.setObject(3, null);
		prep.execute();
		prep.setInt(1, 2);
		prep.setObject(2, new Integer(101), Types.OTHER);
		prep.setObject(3, new Integer(103), Types.OTHER);
		prep.execute();
		PreparedStatement p2 = conn.prepareStatement("SELECT * FROM TEST ORDER BY ID");
		ResultSet rs = p2.executeQuery();
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

	@Test
	public void testDate() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT ?");
		Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
		prep.setObject(1, new java.util.Date(ts.getTime()));
		ResultSet rs = prep.executeQuery();
		rs.next();
		Timestamp ts2 = rs.getTimestamp(1);
		assertEquals(ts.toString(), ts2.toString());
	}

	@Test
	public void testPreparedSubquery() throws SQLException {
		Statement s = conn.createStatement();
		s.executeUpdate("CREATE TABLE TEST(ID IDENTITY, FLAG BIT)");
		s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(0, FALSE)");
		s.executeUpdate("INSERT INTO TEST(ID, FLAG) VALUES(1, FALSE)");
		PreparedStatement u = conn.prepareStatement("SELECT ID, FLAG FROM TEST ORDER BY ID");
		PreparedStatement p = conn.prepareStatement("UPDATE TEST SET FLAG=true WHERE ID=(SELECT ?)");
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

	@Test
	public void testParameterMetaData() throws SQLException {
		PreparedStatement prep = conn.prepareStatement("SELECT ?, ?, ? FROM DUAL");
		ParameterMetaData pm = prep.getParameterMetaData();
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
		} catch (SQLException e) {
			assertKnownException(e);
		}
		try {
			pm.getPrecision(4);
			fail();
		} catch (SQLException e) {
			assertKnownException(e);
		}
		prep.close();
		try {
			pm.getPrecision(1);
			fail();
		} catch (SQLException e) {
			assertKnownException(e);
		}

		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST3(ID INT, NAME VARCHAR(255), DATA DECIMAL(10,2))");
		PreparedStatement prep1 = conn.prepareStatement("UPDATE TEST3 SET ID=?, NAME=?, DATA=?");
		PreparedStatement prep2 = conn.prepareStatement("INSERT INTO TEST3 VALUES(?, ?, ?)");
		checkParameter(prep1, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
		checkParameter(prep1, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
		checkParameter(prep1, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
		checkParameter(prep2, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
		checkParameter(prep2, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
		checkParameter(prep2, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
		PreparedStatement prep3 = conn.prepareStatement("SELECT * FROM TEST3 WHERE ID=? AND NAME LIKE ? AND ?>DATA");
		checkParameter(prep3, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
		checkParameter(prep3, 2, "java.lang.String", 12, "VARCHAR", 0, 0);
		checkParameter(prep3, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
		stat.execute("DROP TABLE TEST3");
	}

	private void checkParameter(PreparedStatement prep, int index, String className, int type, String typeName, int precision, int scale) throws SQLException {
		ParameterMetaData meta = prep.getParameterMetaData();
		assertEquals(className, meta.getParameterClassName(index));
		assertEquals(type, meta.getParameterType(index));
		assertEquals(typeName, meta.getParameterTypeName(index));
		assertEquals(precision, meta.getPrecision(index));
		assertEquals(scale, meta.getScale(index));
	}

	@Test
	public void testLikeIndex() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
		stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
		stat.execute("INSERT INTO TEST VALUES(2, 'World')");
		stat.execute("create index idxname on test(name);");
		PreparedStatement prep, prepExe;

		prep = conn.prepareStatement("EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
		assertEquals(prep.getParameterMetaData().getParameterCount(), 1);
		prepExe = conn.prepareStatement("SELECT * FROM TEST WHERE NAME LIKE ?");
		prep.setString(1, "%orld");
		prepExe.setString(1, "%orld");
		ResultSet rs = prep.executeQuery();
		rs.next();
		String plan = rs.getString(1);
		assertTrue(plan.indexOf("TABLE_SCAN") >= 0);
		rs = prepExe.executeQuery();
		rs.next();
		assertEquals(rs.getString(2), "World");
		assertFalse(rs.next());

		prep.setString(1, "H%");
		prepExe.setString(1, "H%");
		rs = prep.executeQuery();
		rs.next();
		String plan1 = rs.getString(1);
		assertTrue(plan1.indexOf("IDXNAME") >= 0);
		rs = prepExe.executeQuery();
		rs.next();
		assertEquals(rs.getString(2), "Hello");
		assertFalse(rs.next());

		stat.execute("DROP TABLE IF EXISTS TEST");
	}

	@Test
	public void testCasewhen() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST(ID INT)");
		stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
		PreparedStatement prep;
		ResultSet rs;
		prep = conn.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASEWHEN(ID=1, ID, ID)=? GROUP BY ID");
		prep.setInt(1, 1);
		rs = prep.executeQuery();
		rs.next();
		String plan = rs.getString(1);
		trace(plan);
		rs.close();
		prep = conn
		.prepareStatement("EXPLAIN SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
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

		prep = conn
		.prepareStatement("SELECT COUNT(*) FROM TEST WHERE CASE ID WHEN 1 THEN ID WHEN 2 THEN ID ELSE ID END=? GROUP BY ID");
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
			prep = conn.prepareStatement("select ? from dual union select ? from dual");
			fail();
		} catch (SQLException e) {
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

	@Test
	public void testSubquery() throws SQLException {
		Statement stat = conn.createStatement();
		stat.execute("CREATE TABLE TEST(ID INT)");
		stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
		PreparedStatement prep = conn.prepareStatement("select x.id, ? from "
				+ "(select * from test where id in(?, ?)) x where x.id*2 <>  ?");
		assertEquals(prep.getParameterMetaData().getParameterCount(), 4);
		prep.setInt(1, 0);
		prep.setInt(2, 1);
		prep.setInt(3, 2);
		prep.setInt(4, 4);
		ResultSet rs = prep.executeQuery();
		rs.next();
		assertEquals(rs.getInt(1), 1);
		assertEquals(rs.getInt(2), 0);
		assertFalse(rs.next());
		stat.execute("DROP TABLE TEST");
	}

	@Test
	public void testDataTypes() throws SQLException {
		conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		Statement stat = conn.createStatement();
		PreparedStatement prep;
		ResultSet rs;
		trace("Create tables");
		stat.execute("CREATE TABLE T_INT(ID INT PRIMARY KEY,VALUE INT)");
		stat.execute("CREATE TABLE T_VARCHAR(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
		stat.execute("CREATE TABLE T_DECIMAL_0(ID INT PRIMARY KEY,VALUE DECIMAL(30,0))");
		stat.execute("CREATE TABLE T_DECIMAL_10(ID INT PRIMARY KEY,VALUE DECIMAL(20,10))");
		stat.execute("CREATE TABLE T_DATETIME(ID INT PRIMARY KEY,VALUE DATETIME)");
		prep = conn.prepareStatement("INSERT INTO T_INT VALUES(?,?)", ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
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
		prep.setObject(2, new Integer(3));
		prep.executeUpdate();
		prep.setObject(1, "8");
		// should throw an exception
		prep.setObject(2, null);
		// some databases don't allow calling setObject with null (no data type)
		prep.executeUpdate();
		prep.setInt(1, 9);
		prep.setObject(2, new Integer(-4), Types.VARCHAR);
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
		prep.setShort(2, (short) (-30000));
		prep.executeUpdate();
		prep.setInt(1, 18);
		prep.setLong(2, Integer.MAX_VALUE);
		prep.executeUpdate();
		prep.setInt(1, 19);
		prep.setLong(2, Integer.MIN_VALUE);
		prep.executeUpdate();

		assertTrue(stat.execute("SELECT * FROM T_INT ORDER BY ID"));
		rs = stat.getResultSet();
		assertResultSetOrdered(rs, new String[][] { { "1", "0" }, { "2", "-1" }, { "3", "3" }, { "4", null },
				{ "5", "0" }, { "6", "-1" }, { "7", "3" }, { "8", null }, { "9", "-4" }, { "10", "5" }, { "11", null },
				{ "12", "1" }, { "13", "0" }, { "14", "-20" }, { "15", "100" }, { "16", "30000" }, { "17", "-30000" },
				{ "18", "" + Integer.MAX_VALUE }, { "19", "" + Integer.MIN_VALUE }, });

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
		checkBigDecimal(rs, new String[] { "" + Long.MAX_VALUE, "" + Long.MIN_VALUE, "10", "-20", "30", "-40" });

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
		} catch (SQLException e) {
			trace("no error - getMoreResults is supposed to close the result set");
		}
		assertTrue(prep.getUpdateCount() == -1);
		prep = conn.prepareStatement("DELETE FROM TEST");
		prep.executeUpdate();
		assertFalse(prep.getMoreResults());
		assertTrue(prep.getUpdateCount() == -1);
	}

	@Test
	public void testObject() throws SQLException {
		Statement stat = conn.createStatement();
		ResultSet rs;
		stat.execute("DROP TABLE IF EXISTS TEST;");
		stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
		stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
		PreparedStatement prep = conn
		.prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
		prep.setObject(1, new Boolean(true));
		prep.setObject(2, "Abc");
		prep.setObject(3, new BigDecimal("10.2"));
		prep.setObject(4, new Byte((byte) 0xff));
		prep.setObject(5, new Short(Short.MAX_VALUE));
		prep.setObject(6, new Integer(Integer.MIN_VALUE));
		prep.setObject(7, new Long(Long.MAX_VALUE));
		prep.setObject(8, new Float(Float.MAX_VALUE));
		prep.setObject(9, new Double(Double.MAX_VALUE));
		prep.setObject(10, java.sql.Date.valueOf("2001-02-03"));
		prep.setObject(11, java.sql.Time.valueOf("04:05:06"));
		prep.setObject(12, java.sql.Timestamp.valueOf("2001-02-03 04:05:06.123456789"));
		prep.setObject(13, new java.util.Date(java.sql.Date.valueOf("2001-02-03").getTime()));
		byte[] arr_original = new byte[] { 10, 20, 30 };
		prep.setObject(14, arr_original);
		prep.setObject(15, new Character('a'));
		prep.setObject(16, "2001-01-02", Types.DATE);
		// converting to null seems strange...
		prep.setObject(17, "2001-01-02", Types.NULL);
		prep.setObject(18, "3.725", Types.DOUBLE);
		prep.setObject(19, "23:22:21", Types.TIME);
		prep.setObject(20, new java.math.BigInteger("12345"), Types.OTHER);
		rs = prep.executeQuery();
		rs.next();
		assertTrue(rs.getObject(1).equals(new Boolean(true)));
		assertTrue(rs.getObject(2).equals("Abc"));
		assertTrue(rs.getObject(3).equals(new BigDecimal("10.2")));
		assertTrue(rs.getObject(4).equals(new Byte((byte) 0xff)));
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
		byte[] arr = (byte[]) rs.getObject(14);
		assertEquals(arr[0], arr_original[0]);
		assertEquals(arr[1], arr_original[1]);
		assertEquals(arr[2], arr_original[2]);
		assertTrue(rs.getObject(15).equals(new Character('a')));
		assertTrue(rs.getObject(16).equals(java.sql.Date.valueOf("2001-01-02")));
		assertTrue(rs.getObject(17) == null && rs.wasNull());
		assertTrue(rs.getObject(18).equals(new Double(3.725)));
		assertTrue(rs.getObject(19).equals(java.sql.Time.valueOf("23:22:21")));
		assertTrue(rs.getObject(20).equals(new java.math.BigInteger("12345")));

		stat.execute("DROP TABLE TEST");

	}

	private int getLength() throws SQLException {
		return getSize(LOB_SIZE, LOB_SIZE_BIG);
	}

	@Test
	public void testBlob() throws SQLException {
		trace("testBlob");
		Statement stat = conn.createStatement();
		PreparedStatement prep;
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

		int length = getLength();
		byte[] big1 = new byte[length];
		byte[] big2 = new byte[length];
		for (int i = 0; i < big1.length; i++) {
			big1[i] = (byte) ((i * 11) % 254);
			big2[i] = (byte) ((i * 17) % 251);
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
		} catch (IOException e) {
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

	/**
	 * @param big1
	 * @param arr
	 */
	private void checkBytes(byte[] big1, byte[] arr) {
		for (int i = 0; i < arr.length; i++){
			assertEquals(arr[i], big1[i]);
		}
	}

	@Test
	public void testClob() throws SQLException {
		trace("testClob");
		Statement stat = conn.createStatement();
		PreparedStatement prep;
		ResultSet rs;
		stat.execute("CREATE TABLE T_CLOB(ID INT PRIMARY KEY,V1 CLOB,V2 CLOB)");
		StringBuilder asciiBuffer = new StringBuilder();
		int len = getLength();
		for (int i = 0; i < len; i++) {
			asciiBuffer.append((char) ('a' + (i % 20)));
		}
		String ascii1 = asciiBuffer.toString();
		String ascii2 = "Number2 " + ascii1;
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
		assertTrue(prep.getWarnings() == null);
		prep.clearWarnings();
		assertTrue(prep.getWarnings() == null);
		assertTrue(conn == prep.getConnection());
	}

	private void checkBigDecimal(ResultSet rs, String[] value) throws SQLException {
		for (int i = 0; i < value.length; i++) {
			String v = value[i];
			assertTrue(rs.next());
			java.math.BigDecimal x = rs.getBigDecimal(1);
			trace("v=" + v + " x=" + x);
			if (v == null) {
				assertTrue(x == null);
			} else {
				assertTrue(x.compareTo(new java.math.BigDecimal(v)) == 0);
			}
		}
		assertTrue(!rs.next());
	}

}
