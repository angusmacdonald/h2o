/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;

/**
 * Various test cases.
 */
public class TestCases extends TestBase {

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

        testJoinWithView();
        testLobDecrypt();
        testInvalidDatabaseName();
        testReuseSpace();
        testDeleteGroup();
        testDisconnect();
        testExecuteTrace();
        if (config.memory || config.logMode == 0) { return; }
        testReservedKeywordReconnect();
        testSpecialSQL();
        testUpperCaseLowerCaseDatabase();
        testManualCommitSet();
        testSchemaIdentityReconnect();
        testAlterTableReconnect();
        testPersistentSettings();
        testInsertSelectUnion();
        testViewReconnect();
        testDefaultQueryReconnect();
        testBigString();
        testRenameReconnect();
        testAllSizes();
        testCreateDrop();
        testPolePos();
        testQuick();
        testMutableObjects();
        testSelectForUpdate();
        testDoubleRecovery();
        testConstraintReconnect();
        testCollation();
        deleteDb("cases");
    }

    private void testJoinWithView() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        conn.createStatement().execute("create table t(i identity, n varchar) as select 1, 'x'");
        final PreparedStatement prep = conn.prepareStatement("select 1 from dual " + "inner join(select n from t where i=?) a on a.n='x' " + "inner join(select n from t where i=?) b on b.n='x'");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.execute();
        conn.close();
    }

    private void testLobDecrypt() throws SQLException, IOException {

        final Connection conn = getConnection("cases");
        final String key = "key";
        final String value = "Hello World";
        final PreparedStatement prep = conn.prepareStatement("CALL ENCRYPT('AES', RAWTOHEX(?), STRINGTOUTF8(?))");
        prep.setCharacterStream(1, new StringReader(key), -1);
        prep.setCharacterStream(2, new StringReader(value), -1);
        final ResultSet rs = prep.executeQuery();
        rs.next();
        final String encrypted = rs.getString(1);
        final PreparedStatement prep2 = conn.prepareStatement("CALL TRIM(CHAR(0) FROM UTF8TOSTRING(DECRYPT('AES', RAWTOHEX(?), ?)))");
        prep2.setCharacterStream(1, new StringReader(key), -1);
        prep2.setCharacterStream(2, new StringReader(encrypted), -1);
        final ResultSet rs2 = prep2.executeQuery();
        rs2.first();
        final String decrypted = rs2.getString(1);
        prep2.close();
        assertEquals(value, decrypted);
        conn.close();
    }

    private void testReservedKeywordReconnect() throws SQLException, IOException {

        if (config.memory) { return; }
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table \"UNIQUE\"(\"UNIQUE\" int)");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("select \"UNIQUE\" from \"UNIQUE\"");
        stat.execute("drop table \"UNIQUE\"");
        conn.close();
    }

    private void testInvalidDatabaseName() throws SQLException, IOException {

        if (config.memory) { return; }
        try {
            getConnection("cases/");
            fail();
        }
        catch (final SQLException e) {
            assertEquals(ErrorCode.INVALID_DATABASE_NAME_1, e.getErrorCode());
        }
    }

    private void testReuseSpace() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        final int tableCount = getSize(2, 5);
        for (int i = 0; i < tableCount; i++) {
            stat.execute("create table t" + i + "(data varchar)");
        }
        final Random random = new Random(1);
        final int len = getSize(50, 500);
        for (int i = 0; i < len; i++) {
            final String table = "t" + random.nextInt(tableCount);
            String sql;
            if (random.nextBoolean()) {
                sql = "insert into " + table + " values(space(100000))";
            }
            else {
                sql = "delete from " + table;
            }
            stat.execute(sql);
            stat.execute("script to '" + baseDir + "/test.sql'");
        }
        conn.close();
    }

    private void testDeleteGroup() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("set max_memory_rows 2");
        stat.execute("create table test(id int primary key, x int)");
        stat.execute("insert into test values(0, 0), (1, 1), (2, 2)");
        stat.execute("delete from test where id not in (select min(x) from test group by id)");
        conn.close();
    }

    private void testSpecialSQL() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        try {
            stat.execute("create table address(id identity, name varchar check? instr(value, '@') > 1)");
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        stat.execute("SET AUTOCOMMIT OFF; \n//create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF;\n//create sequence if not exists object_id;\n");
        stat.execute("SET AUTOCOMMIT OFF; //create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF //create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF//create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; \n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;\n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF; ///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF;///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF \n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF\n///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF ///create sequence if not exists object_id;");
        stat.execute("SET AUTOCOMMIT OFF///create sequence if not exists object_id;");
        conn.close();
    }

    private void testUpperCaseLowerCaseDatabase() throws SQLException, IOException {

        if (File.separatorChar != '\\') { return; }
        deleteDb("cases");
        deleteDb("CaSeS");
        Connection conn, conn2;
        ResultSet rs;
        conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CHECKPOINT");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("CHECKPOINT");

        conn2 = getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn2.close();

        conn.close();

        conn = getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn.close();

        conn = getConnection("CaSeS");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        assertTrue(rs.next());
        conn.close();

        deleteDb("cases");
        deleteDb("CaSeS");

    }

    private void testManualCommitSet() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Connection conn2 = getConnection("cases");
        conn.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn.createStatement().execute("SET MODE REGULAR");
        conn2.createStatement().execute("SET MODE REGULAR");
        conn.close();
        conn2.close();
    }

    private void testSchemaIdentityReconnect() throws SQLException, IOException {

        deleteDb("cases");
        Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("create schema s authorization sa");
        stat.execute("create table s.test(id identity)");
        conn.close();
        conn = getConnection("cases");
        final ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM S.TEST");
        while (rs.next()) {
            // ignore
        }
        conn.close();
    }

    private void testDisconnect() throws Exception {

        if (config.networked || config.codeCoverage) { return; }
        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY)");
        for (int i = 0; i < 1000; i++) {
            stat.execute("INSERT INTO TEST() VALUES()");
        }
        final SQLException[] stopped = new SQLException[1];
        final Thread t = new Thread(new Runnable() {

            @Override
            public void run() {

                try {
                    long time = System.currentTimeMillis();
                    final ResultSet rs = stat.executeQuery("SELECT MAX(T.ID) FROM TEST T, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST, TEST");
                    rs.next();
                    time = System.currentTimeMillis() - time;
                    TestBase.logError("query was too quick; result: " + rs.getInt(1) + " time:" + time, null);
                }
                catch (final SQLException e) {
                    stopped[0] = e;
                    // ok
                }
            }
        });
        t.start();
        Thread.sleep(300);
        long time = System.currentTimeMillis();
        conn.close();
        t.join(5000);
        if (stopped[0] == null) {
            fail("query still running");
        }
        else {
            assertKnownException(stopped[0]);
        }
        time = System.currentTimeMillis() - time;
        if (time > 5000) {
            fail("closing took " + time);
        }
        deleteDb("cases");
    }

    private void testExecuteTrace() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("SELECT ? FROM DUAL {1: 'Hello'}");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        assertFalse(rs.next());

        rs = stat.executeQuery("SELECT ? FROM DUAL UNION ALL SELECT ? FROM DUAL {1: 'Hello', 2:'World' }");
        rs.next();
        assertEquals("Hello", rs.getString(1));
        rs.next();
        assertEquals("World", rs.getString(1));
        assertFalse(rs.next());

        conn.close();
    }

    private void testAlterTableReconnect() throws SQLException, IOException {

        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id identity);");
        stat.execute("insert into test values(1);");
        try {
            stat.execute("alter table test add column name varchar not null;");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        conn.close();
        conn = getConnection("cases");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getString(1), "1");
        assertFalse(rs.next());
        stat = conn.createStatement();
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        stat.execute("alter table test alter column id set default 'x'");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getString(1), "1");
        assertFalse(rs.next());
        stat.execute("drop table test");
        stat.execute("create table test(id identity)");
        stat.execute("insert into test values(1)");
        try {
            stat.execute("alter table test alter column id date");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        conn.close();
        conn = getConnection("cases");
        rs = conn.createStatement().executeQuery("SELECT * FROM TEST");
        rs.next();
        assertEquals(rs.getString(1), "1");
        assertFalse(rs.next());
        conn.close();
    }

    private void testCollation() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("SET COLLATION ENGLISH STRENGTH PRIMARY");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'WORLD'), (4, 'HELLO')");
        stat.execute("create index idxname on test(name)");
        ResultSet rs;
        rs = stat.executeQuery("select name from test order by name");
        rs.next();
        assertEquals(rs.getString(1), "Hello");
        rs.next();
        assertEquals(rs.getString(1), "HELLO");
        rs.next();
        assertEquals(rs.getString(1), "World");
        rs.next();
        assertEquals(rs.getString(1), "WORLD");
        rs = stat.executeQuery("select name from test where name like 'He%'");
        rs.next();
        assertEquals(rs.getString(1), "Hello");
        rs.next();
        assertEquals(rs.getString(1), "HELLO");
        conn.close();
    }

    private void testPersistentSettings() throws SQLException, IOException {

        deleteDb("cases");
        Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("SET COLLATION de_DE");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("CREATE INDEX IDXNAME ON TEST(NAME)");
        // \u00f6 = oe
        stat.execute("INSERT INTO TEST VALUES(1, 'B\u00f6hlen'), (2, 'Bach'), (3, 'Bucher')");
        conn.close();
        conn = getConnection("cases");
        final ResultSet rs = conn.createStatement().executeQuery("SELECT NAME FROM TEST ORDER BY NAME");
        rs.next();
        assertEquals(rs.getString(1), "Bach");
        rs.next();
        assertEquals(rs.getString(1), "B\u00f6hlen");
        rs.next();
        assertEquals(rs.getString(1), "Bucher");
        conn.close();
    }

    private void testInsertSelectUnion() throws SQLException, IOException {

        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ORDER_ID INT PRIMARY KEY, ORDER_DATE DATETIME, USER_ID INT ," + "DESCRIPTION VARCHAR, STATE VARCHAR, TRACKING_ID VARCHAR)");
        final Timestamp orderDate = Timestamp.valueOf("2005-05-21 17:46:00");
        final String sql = "insert into TEST (ORDER_ID,ORDER_DATE,USER_ID,DESCRIPTION,STATE,TRACKING_ID) " + "select cast(? as int),cast(? as date),cast(? as int),cast(? as varchar),cast(? as varchar),cast(? as varchar) union all select ?,?,?,?,?,?";
        final PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 5555);
        ps.setTimestamp(2, orderDate);
        ps.setInt(3, 2222);
        ps.setString(4, "test desc");
        ps.setString(5, "test_state");
        ps.setString(6, "testid");
        ps.setInt(7, 5556);
        ps.setTimestamp(8, orderDate);
        ps.setInt(9, 2222);
        ps.setString(10, "test desc");
        ps.setString(11, "test_state");
        ps.setString(12, "testid");
        assertEquals(ps.executeUpdate(), 2);
        ps.close();
        conn.close();
    }

    private void testViewReconnect() throws SQLException, IOException {

        trace("testViewReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int)");
        stat.execute("create view abc as select * from test");
        stat.execute("drop table test");
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        try {
            stat.execute("select * from abc");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        conn.close();
    }

    private void testDefaultQueryReconnect() throws SQLException, IOException {

        trace("testDefaultQueryReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("create table parent(id int)");
        stat.execute("insert into parent values(1)");
        stat.execute("create table test(id int default (select max(id) from parent), name varchar)");

        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        conn.setAutoCommit(false);
        stat.execute("insert into parent values(2)");
        stat.execute("insert into test(name) values('test')");
        final ResultSet rs = stat.executeQuery("select * from test");
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());
        conn.close();
    }

    private void testBigString() throws SQLException, IOException {

        trace("testBigString");
        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT, TEXT VARCHAR, TEXT_C CLOB)");
        final PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
        final int len = getSize(1000, 66000);
        final char[] buff = new char[len];

        // The UCS code values 0xd800-0xdfff (UTF-16 surrogates)
        // as well as 0xfffe and 0xffff (UCS non-characters)
        // should not appear in conforming UTF-8 streams.
        // (String.getBytes("UTF-8") only returns 1 byte for 0xd800-0xdfff)

        final Random random = new Random();
        random.setSeed(1);
        for (int i = 0; i < len; i++) {
            char c;
            do {
                c = (char) random.nextInt();
            }
            while (c >= 0xd800 && c <= 0xdfff);
            buff[i] = c;
        }
        final String big = new String(buff);
        prep.setInt(1, 1);
        prep.setString(2, big);
        prep.setString(3, big);
        prep.execute();
        prep.setInt(1, 2);
        prep.setCharacterStream(2, new StringReader(big), 0);
        prep.setCharacterStream(3, new StringReader(big), 0);
        prep.execute();
        final ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), big);
        assertEquals(readString(rs.getCharacterStream(2)), big);
        assertEquals(rs.getString(3), big);
        assertEquals(readString(rs.getCharacterStream(3)), big);
        rs.next();
        assertEquals(rs.getInt(1), 2);
        assertEquals(rs.getString(2), big);
        assertEquals(readString(rs.getCharacterStream(2)), big);
        assertEquals(rs.getString(3), big);
        assertEquals(readString(rs.getCharacterStream(3)), big);
        rs.next();
        assertFalse(rs.next());
        conn.close();
    }

    private void testConstraintReconnect() throws SQLException, IOException {

        trace("testConstraintReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("drop table if exists parent");
        stat.execute("drop table if exists child");
        stat.execute("create table parent(id int)");
        stat.execute("create table child(c_id int, p_id int, foreign key(p_id) references parent(id))");
        stat.execute("insert into parent values(1), (2)");
        stat.execute("insert into child values(1, 1)");
        stat.execute("insert into child values(2, 2)");
        stat.execute("insert into child values(3, 2)");
        stat.execute("delete from child");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testDoubleRecovery() throws SQLException, IOException {

        if (config.networked) { return; }
        trace("testDoubleRecovery");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.setAutoCommit(false);
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        crash(conn);

        conn = getConnection("cases");
        stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        stat.execute("INSERT INTO TEST VALUES(3, 'Break')");
        crash(conn);

        conn = getConnection("cases");
        stat = conn.createStatement();
        final ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals("Hello", rs.getString(2));
        rs.next();
        assertEquals(3, rs.getInt(1));
        assertEquals("Break", rs.getString(2));
        conn.close();
    }

    private void testRenameReconnect() throws SQLException, IOException {

        trace("testRenameReconnect");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        conn.createStatement().execute("CREATE TABLE TEST_SEQ(ID INT IDENTITY, NAME VARCHAR(255))");
        conn.createStatement().execute("CREATE TABLE TEST(ID INT PRIMARY KEY)");
        conn.createStatement().execute("ALTER TABLE TEST RENAME TO TEST2");
        conn.createStatement().execute("CREATE TABLE TEST_B(ID INT PRIMARY KEY, NAME VARCHAR, UNIQUE(NAME))");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("INSERT INTO TEST_SEQ(NAME) VALUES('Hi')");
        ResultSet rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        assertEquals(1, rs.getInt(1));
        conn.createStatement().execute("SELECT * FROM TEST2");
        conn.createStatement().execute("SELECT * FROM TEST_B");
        conn.createStatement().execute("ALTER TABLE TEST_B RENAME TO TEST_B2");
        conn.close();
        conn = getConnection("cases");
        conn.createStatement().execute("SELECT * FROM TEST_B2");
        conn.createStatement().execute("INSERT INTO TEST_SEQ(NAME) VALUES('World')");
        rs = conn.createStatement().executeQuery("CALL IDENTITY()");
        rs.next();
        assertEquals(2, rs.getInt(1));
        conn.close();
    }

    private void testAllSizes() throws SQLException, IOException {

        trace("testAllSizes");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(A INT, B INT, C INT, DATA VARCHAR)");
        final int increment = getSize(100, 1);
        for (int i = 1; i < 500; i += increment) {
            final StringBuilder buff = new StringBuilder();
            buff.append("CREATE TABLE TEST");
            for (int j = 0; j < i; j++) {
                buff.append('a');
            }
            buff.append("(ID INT)");
            final String sql = buff.toString();
            stat.execute(sql);
            stat.execute("INSERT INTO TEST VALUES(" + i + ", 0, 0, '" + sql + "')");
        }
        conn.close();
        conn = getConnection("cases");
        stat = conn.createStatement();
        final ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        while (rs.next()) {
            final int id = rs.getInt(1);
            final String s = rs.getString("DATA");
            if (!s.endsWith(")")) {
                fail("id=" + id);
            }
        }
        conn.close();
    }

    private void testSelectForUpdate() throws SQLException, IOException {

        trace("testSelectForUpdate");
        deleteDb("cases");
        final Connection conn1 = getConnection("cases");
        final Statement stat1 = conn1.createStatement();
        stat1.execute("CREATE TABLE TEST(ID INT)");
        stat1.execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);
        stat1.execute("SELECT * FROM TEST FOR UPDATE");
        final Connection conn2 = getConnection("cases");
        final Statement stat2 = conn2.createStatement();
        try {
            stat2.execute("UPDATE TEST SET ID=2");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        conn1.commit();
        stat2.execute("UPDATE TEST SET ID=2");
        conn1.close();
        conn2.close();
    }

    private void testMutableObjects() throws SQLException, IOException {

        trace("testMutableObjects");
        deleteDb("cases");
        final Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT, D DATE, T TIME, TS TIMESTAMP)");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', '20:00:00', '2002-02-02 22:22:22.2')");
        stat.execute("INSERT INTO TEST VALUES(1, '2001-01-01', '20:00:00', '2002-02-02 22:22:22.2')");
        final ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        rs.next();
        final Date d1 = rs.getDate("D");
        final Time t1 = rs.getTime("T");
        final Timestamp ts1 = rs.getTimestamp("TS");
        rs.next();
        final Date d2 = rs.getDate("D");
        final Time t2 = rs.getTime("T");
        final Timestamp ts2 = rs.getTimestamp("TS");
        assertTrue(ts1 != ts2);
        assertTrue(d1 != d2);
        assertTrue(t1 != t2);
        assertTrue(t2 != rs.getObject("T"));
        assertTrue(d2 != rs.getObject("D"));
        assertTrue(ts2 != rs.getObject("TS"));
        assertFalse(rs.next());
        conn.close();
    }

    private void testCreateDrop() throws SQLException, IOException {

        trace("testCreateDrop");
        deleteDb("cases");
        Connection conn = getConnection("cases");
        final Statement stat = conn.createStatement();
        stat.execute("create table employee(id int, " + "firstName VARCHAR(50), " + "salary decimal(10, 2), " + "superior_id int, " + "CONSTRAINT PK_employee PRIMARY KEY (id), " + "CONSTRAINT FK_superior FOREIGN KEY (superior_id) " + "REFERENCES employee(ID))");
        stat.execute("DROP TABLE employee");
        conn.close();
        conn = getConnection("cases");
        conn.close();
    }

    private void testPolePos() throws SQLException, IOException {

        trace("testPolePos");
        // poleposition-0.20

        Connection c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), firstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        final PreparedStatement p15 = c0.prepareStatement("insert into australia (id,Name,firstName,Points,LicenseID) values (?,?,?,?,?)");
        int len = getSize(1, 1000);
        for (int i = 0; i < len; i++) {
            p15.setInt(1, i);
            p15.setString(2, "Pilot_" + i);
            p15.setString(3, "Herkules");
            p15.setInt(4, i);
            p15.setInt(5, i);
            p15.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        // c0=getConnection("cases");
        // c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        // c0.createStatement().executeQuery("select * from australia");
        // c0.createStatement().executeQuery("select * from australia");
        // c0.close();

        // c0=getConnection("cases");
        // c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        // c0.createStatement().executeUpdate("COMMIT");
        // c0.createStatement().executeUpdate("delete from australia");
        // c0.createStatement().executeUpdate("COMMIT");
        // c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        c0.createStatement().executeUpdate("drop table australia");
        c0.createStatement().executeUpdate("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), firstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("SET AUTOCOMMIT FALSE");
        final PreparedStatement p65 = c0.prepareStatement("insert into australia (id,Name,FirstName,Points,LicenseID) values (?,?,?,?,?)");
        len = getSize(1, 1000);
        for (int i = 0; i < len; i++) {
            p65.setInt(1, i);
            p65.setString(2, "Pilot_" + i);
            p65.setString(3, "Herkules");
            p65.setInt(4, i);
            p65.setInt(5, i);
            p65.executeUpdate();
        }
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.createStatement().executeUpdate("COMMIT");
        c0.close();

        c0 = getConnection("cases");
        c0.close();
    }

    private void testQuick() throws SQLException, IOException {

        trace("testQuick");
        deleteDb("cases");

        Connection c0 = getConnection("cases");
        c0.createStatement().executeUpdate("create table test (ID  int PRIMARY KEY)");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.createStatement().executeUpdate("drop table test");
        c0.createStatement().executeUpdate("create table test (ID  int PRIMARY KEY)");
        c0.close();

        c0 = getConnection("cases");
        c0.createStatement().executeUpdate("insert into test values(1)");
        c0.close();

        c0 = getConnection("cases");
        c0.close();
    }

}
