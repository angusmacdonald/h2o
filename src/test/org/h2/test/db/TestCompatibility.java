/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.engine.Constants;
import org.h2.test.TestBase;

/**
 * Tests the compatibility with other databases.
 */
public class TestCompatibility extends TestBase {

    private Connection conn;

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

        deleteDb("compatibility");
        conn = getConnection("compatibility");

        testDomain();
        testColumnAlias();
        testUniqueIndexSingleNull();
        testUniqueIndexOracle();
        testHsqlDb();
        testMySQL();

        conn.close();
        deleteDb("compatibility");
    }

    private void testDomain() throws SQLException, IOException {

        if (config.memory) { return; }
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key) as select 1");
        try {
            stat.execute("create domain int as varchar");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        conn.close();
        conn = getConnection("compatibility");
        stat = conn.createStatement();
        stat.execute("insert into test values(2)");
        stat.execute("drop table test");
    }

    private void testColumnAlias() throws SQLException {

        final Statement stat = conn.createStatement();
        final String[] modes = new String[]{"PostgreSQL", "MySQL", "HSQLDB", "MSSQLServer", "Derby", "Oracle", "Regular"};
        String columnAlias;
        if (Constants.VERSION_MINOR == 0) {
            columnAlias = "MySQL";
        }
        else {
            columnAlias = "MySQL,Regular";
        }
        stat.execute("CREATE TABLE TEST(ID INT)");
        for (final String mode : modes) {
            stat.execute("SET MODE " + mode);
            final ResultSet rs = stat.executeQuery("SELECT ID I FROM TEST");
            final ResultSetMetaData meta = rs.getMetaData();
            final String columnName = meta.getColumnName(1);
            final String tableName = meta.getTableName(1);
            if ("ID".equals(columnName) && "TEST".equals(tableName)) {
                assertTrue(mode + " mode should not support columnAlias", columnAlias.indexOf(mode) >= 0);
            }
            else if ("I".equals(columnName) && tableName == null) {
                assertTrue(mode + " mode should support columnAlias", columnAlias.indexOf(mode) < 0);
            }
            else {
                fail();
            }
        }
        stat.execute("DROP TABLE TEST");
    }

    private void testUniqueIndexSingleNull() throws SQLException {

        final Statement stat = conn.createStatement();
        final String[] modes = new String[]{"PostgreSQL", "MySQL", "HSQLDB", "MSSQLServer", "Derby", "Oracle", "Regular"};
        final String multiNull = "PostgreSQL,MySQL,Oracle,Regular";
        for (final String mode : modes) {
            stat.execute("SET MODE " + mode);
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("CREATE UNIQUE INDEX IDX_ID_U ON TEST(ID)");
            try {
                stat.execute("INSERT INTO TEST VALUES(1), (2), (NULL), (NULL)");
                assertTrue(mode + " mode should not support multiple NULL", multiNull.indexOf(mode) >= 0);
            }
            catch (final SQLException e) {
                assertTrue(mode + " mode should support multiple NULL", multiNull.indexOf(mode) < 0);
            }
            stat.execute("DROP TABLE TEST");
        }
    }

    private void testUniqueIndexOracle() throws SQLException {

        final Statement stat = conn.createStatement();
        stat.execute("SET MODE ORACLE");
        stat.execute("create table t2(c1 int, c2 int)");
        stat.execute("create unique index i2 on t2(c1, c2)");
        stat.execute("insert into t2 values (null, 1)");
        try {
            stat.execute("insert into t2 values (null, 1)");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (null, null)");
        stat.execute("insert into t2 values (1, null)");
        try {
            stat.execute("insert into t2 values (1, null)");
            fail();
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        stat.execute("DROP TABLE T2");
    }

    private void testHsqlDb() throws SQLException {

        final Statement stat = conn.createStatement();
        stat.execute("DROP TABLE TEST IF EXISTS; CREATE TABLE TEST(ID INT PRIMARY KEY); ");
        stat.execute("CALL CURRENT_TIME");
        stat.execute("CALL CURRENT_TIMESTAMP");
        stat.execute("CALL CURRENT_DATE");
        stat.execute("CALL SYSDATE");
        stat.execute("CALL TODAY");

        stat.execute("DROP TABLE TEST IF EXISTS");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        final PreparedStatement prep = conn.prepareStatement("SELECT LIMIT ? 1 ID FROM TEST");
        prep.setInt(1, 2);
        prep.executeQuery();
        stat.execute("DROP TABLE TEST IF EXISTS");

    }

    private void testMySQL() throws SQLException {

        final Statement stat = conn.createStatement();
        stat.execute("SELECT 1");
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World')");
        org.h2.mode.FunctionsMySQL.register(conn);
        assertResult(stat, "SELECT UNIX_TIMESTAMP('2007-11-30 10:30:19Z')", "1196418619");
        assertResult(stat, "SELECT UNIX_TIMESTAMP(FROM_UNIXTIME(1196418619))", "1196418619");
        assertResult(stat, "SELECT FROM_UNIXTIME(1196300000, '%Y %M')", "2007 November");
        assertResult(stat, "SELECT DATE('2003-12-31 11:02:03')", "2003-12-31");

    }

}
