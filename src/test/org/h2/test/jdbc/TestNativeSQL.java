/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.h2.test.TestBase;

/**
 * Tests the Connection.nativeSQL method.
 */
public class TestNativeSQL extends TestBase {

    private static final String[] PAIRS = new String[]{"CREATE TABLE TEST(ID INT PRIMARY KEY)", "CREATE TABLE TEST(ID INT PRIMARY KEY)",

    "INSERT INTO TEST VALUES(1)", "INSERT INTO TEST VALUES(1)", "SELECT '{nothing}' FROM TEST", "SELECT '{nothing}' FROM TEST",

    "SELECT '{fn ABS(1)}' FROM TEST", "SELECT '{fn ABS(1)}' FROM TEST",

    "SELECT {d '2001-01-01'} FROM TEST", "SELECT    '2001-01-01'  FROM TEST",

    "SELECT {t '20:00:00'} FROM TEST", "SELECT    '20:00:00'  FROM TEST",

    "SELECT {ts '2001-01-01 20:00:00'} FROM TEST", "SELECT     '2001-01-01 20:00:00'  FROM TEST",

    "SELECT {fn CONCAT('{fn x}','{oj}')} FROM TEST", "SELECT     CONCAT('{fn x}','{oj}')  FROM TEST",

    "SELECT * FROM {oj TEST T1 LEFT OUTER JOIN TEST T2 ON T1.ID=T2.ID}", "SELECT * FROM     TEST T1 LEFT OUTER JOIN TEST T2 ON T1.ID=T2.ID ",

    "SELECT * FROM TEST WHERE '{' LIKE '{{' {escape '{'}", "SELECT * FROM TEST WHERE '{' LIKE '{{'  escape '{' ",

    "SELECT * FROM TEST WHERE '}' LIKE '}}' {escape '}'}", "SELECT * FROM TEST WHERE '}' LIKE '}}'  escape '}' ",

    "{call TEST('}')}", " call TEST('}') ",

    "{?= call TEST('}')}", "    call TEST('}') ",

    "{? = call TEST('}')}", "     call TEST('}') ",

    "{{{{this is a bug}", null,};

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

        deleteDb("nativeSql");
        conn = getConnection("nativeSql");
        testPairs();
        testCases();
        testRandom();
        testQuotes();
        conn.close();
        assertTrue(conn.isClosed());
        deleteDb("nativeSql");
    }

    private void testQuotes() throws SQLException {

        final Statement stat = conn.createStatement();
        final Random random = new Random(1);
        final String s = "'\"$/-* \n";
        for (int i = 0; i < 200; i++) {
            final StringBuilder buffQuoted = new StringBuilder();
            final StringBuilder buffRaw = new StringBuilder();
            if (random.nextBoolean()) {
                buffQuoted.append("'");
                for (int j = 0; j < 10; j++) {
                    final char c = s.charAt(random.nextInt(s.length()));
                    if (c == '\'') {
                        buffQuoted.append('\'');
                    }
                    buffQuoted.append(c);
                    buffRaw.append(c);
                }
                buffQuoted.append("'");
            }
            else {
                buffQuoted.append("$$");
                for (int j = 0; j < 10; j++) {
                    final char c = s.charAt(random.nextInt(s.length()));
                    buffQuoted.append(c);
                    buffRaw.append(c);
                    if (c == '$') {
                        buffQuoted.append(' ');
                        buffRaw.append(' ');
                    }
                }
                buffQuoted.append("$$");
            }
            final String sql = "CALL " + buffQuoted.toString();
            final ResultSet rs = stat.executeQuery(sql);
            rs.next();
            final String raw = buffRaw.toString();
            assertEquals(raw, rs.getString(1));
        }
    }

    private void testRandom() throws SQLException {

        final Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            final StringBuilder buff = new StringBuilder("{oj }");
            final String s = "{}\'\"-/*$ $-";
            for (int j = random.nextInt(30); j > 0; j--) {
                buff.append(s.charAt(random.nextInt(s.length())));
            }
            final String sql = buff.toString();
            try {
                conn.nativeSQL(sql);
            }
            catch (final SQLException e) {
                assertKnownException(sql, e);
            }
        }
        String smallest = null;
        for (int i = 0; i < 1000; i++) {
            final StringBuilder buff = new StringBuilder("{oj }");
            for (int j = random.nextInt(10); j > 0; j--) {
                String s;
                switch (random.nextInt(7)) {
                    case 0:
                        buff.append(" $$");
                        s = "{}\'\"-/* a\n";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("$$");
                        break;
                    case 1:
                        buff.append("'");
                        s = "{}\"-/*$ a\n";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("'");
                        break;
                    case 2:
                        buff.append("\"");
                        s = "{}'-/*$ a\n";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("\"");
                        break;
                    case 3:
                        buff.append("/*");
                        s = "{}'\"-/$ a\n";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("*/");
                        break;
                    case 4:
                        buff.append("--");
                        s = "{}'\"-/$ a";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("\n");
                        break;
                    case 5:
                        buff.append("//");
                        s = "{}'\"-/$ a";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        buff.append("\n");
                        break;
                    case 6:
                        s = " a\n";
                        for (int k = random.nextInt(5); k > 0; k--) {
                            buff.append(s.charAt(random.nextInt(s.length())));
                        }
                        break;
                    default:
                }
            }
            final String sql = buff.toString();
            try {
                conn.nativeSQL(sql);
            }
            catch (final Exception e) {
                if (smallest == null || sql.length() < smallest.length()) {
                    smallest = sql;
                }
            }
        }
        if (smallest != null) {
            conn.nativeSQL(smallest);
        }
    }

    private void testPairs() throws SQLException {

        for (int i = 0; i < PAIRS.length; i += 2) {
            test(conn, PAIRS[i], PAIRS[i + 1]);
        }
    }

    private void testCases() throws SQLException {

        conn.nativeSQL("TEST");
        conn.nativeSQL("TEST--testing");
        conn.nativeSQL("TEST--testing{oj }");
        conn.nativeSQL("TEST/*{fn }*/");
        conn.nativeSQL("TEST//{fn }");
        conn.nativeSQL("TEST-TEST/TEST/*TEST*/TEST--\rTEST--{fn }");
        conn.nativeSQL("TEST-TEST//TEST");
        conn.nativeSQL("'{}' '' \"1\" \"\"\"\"");
        conn.nativeSQL("{?= call HELLO{t '10'}}");
        conn.nativeSQL("TEST 'test'{OJ OUTER JOIN}'test'{oj OUTER JOIN}");
        conn.nativeSQL("{call {ts '2001-01-10'}}");
        conn.nativeSQL("call ? { 1: '}' };");
        conn.nativeSQL("TEST TEST TEST TEST TEST 'TEST' TEST \"TEST\"");
        conn.nativeSQL("TEST TEST TEST  'TEST' TEST \"TEST\"");
        final Statement stat = conn.createStatement();
        stat.setEscapeProcessing(true);
        stat.execute("CALL {d '2001-01-01'}");
        stat.setEscapeProcessing(false);
        try {
            stat.execute("CALL {d '2001-01-01'} // this is a test");
            fail("expected error if setEscapeProcessing=false");
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
        assertFalse(conn.isClosed());
    }

    private void test(final Connection conn, final String original, final String expected) throws SQLException {

        trace("original: <" + original + ">");
        trace("expected: <" + expected + ">");
        try {
            final String result = conn.nativeSQL(original);
            trace("result: <" + result + ">");
            assertEquals(expected, result);
        }
        catch (final SQLException e) {
            assertEquals(expected, null);
            assertKnownException(e);
            trace("got exception, good");
        }
    }

}
