/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import org.h2.test.TestBase;
import org.h2.tools.Csv;
import org.h2.util.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * CSVREAD and CSVWRITE tests.
 * 
 * @author Thomas Mueller
 * @author Sylvain Cuaz (testNull)
 * 
 */
public class TestCsv extends TestBase {

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

        testSpaceSeparated();
        testNull();
        testRandomData();
        testEmptyFieldDelimiter();
        testFieldDelimiter();
        testAsTable();
        testWriteRead();
        testRead();
        testPipe();
        deleteDb("csv");
    }

    private void testSpaceSeparated() throws SQLException, IOException {

        deleteDb("csv");
        final File f = new File(baseDir + "/testSpace.csv");
        FileUtils.delete(f.getAbsolutePath());

        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("create temporary table test (a int, b int, c int)");
        stat.execute("insert into test values(1,2,3)");
        stat.execute("insert into test values(4,null,5)");
        stat.execute("call csvwrite('test.tsv','select * from test',null,' ')");
        final ResultSet rs1 = stat.executeQuery("select * from test");
        assertResultSetOrdered(rs1, new String[][]{new String[]{"1", "2", "3"}, new String[]{"4", null, "5"}});
        final ResultSet rs2 = stat.executeQuery("select * from csvread('test.tsv',null,null,' ')");
        assertResultSetOrdered(rs2, new String[][]{new String[]{"1", "2", "3"}, new String[]{"4", null, "5"}});
        conn.close();
        FileUtils.delete(f.getAbsolutePath());
    }

    /**
     * Test custom NULL string.
     */
    private void testNull() throws Exception {

        deleteDb("csv");

        final File f = new File(baseDir + "/testNull.csv");
        FileUtils.delete(f.getAbsolutePath());

        final RandomAccessFile file = new RandomAccessFile(f, "rw");
        final String csvContent = "\"A\",\"B\",\"C\",\"D\"\n\\N,\"\",\"\\N\",";
        file.write(csvContent.getBytes("UTF-8"));
        file.close();
        final Csv csv = Csv.getInstance();
        csv.setNullString("\\N");
        final ResultSet rs = csv.read(f.getPath(), null, "UTF8");
        final ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 4);
        assertEquals(meta.getColumnLabel(1), "A");
        assertEquals(meta.getColumnLabel(2), "B");
        assertEquals(meta.getColumnLabel(3), "C");
        assertEquals(meta.getColumnLabel(4), "D");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), null);
        assertEquals(rs.getString(2), "");
        // null is never quoted
        assertEquals(rs.getString(3), "\\N");
        // an empty string is always parsed as null
        assertEquals(rs.getString(4), null);
        assertFalse(rs.next());

        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + f.getPath() + "', 'select NULL as a, '''' as b, ''\\N'' as c, NULL as d', 'UTF8', ',', '\"', NULL, '\\N', '\n')");
        final FileReader reader = new FileReader(f);
        // on read, an empty string is treated like null,
        // but on write a null is always written with the nullString
        final String data = IOUtils.readStringAndClose(reader, -1);
        assertEquals(csvContent + "\\N", data.trim());
        conn.close();

        FileUtils.delete(f.getAbsolutePath());
    }

    private void testRandomData() throws SQLException, IOException {

        deleteDb("csv");
        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("drop table if exists test");
        stat.execute("create table test(a varchar, b varchar)");
        final int len = getSize(1000, 10000);
        final PreparedStatement prep = conn.prepareStatement("insert into test values(?, ?)");
        final ArrayList list = new ArrayList();
        final Random random = new Random(1);
        for (int i = 0; i < len; i++) {
            final String a = randomData(random), b = randomData(random);
            prep.setString(1, a);
            prep.setString(2, b);
            list.add(new String[]{a, b});
            prep.execute();
        }
        stat.execute("CALL CSVWRITE('" + baseDir + "/test.csv', 'SELECT * FROM test', 'UTF-8', '|', '#')");
        final Csv csv = Csv.getInstance();
        csv.setFieldSeparatorRead('|');
        csv.setFieldDelimiter('#');
        final ResultSet rs = csv.read(baseDir + "/test.csv", null, "UTF-8");
        for (int i = 0; i < len; i++) {
            assertTrue(rs.next());
            final String[] pair = (String[]) list.get(i);
            assertEquals(pair[0], rs.getString(1));
            assertEquals(pair[1], rs.getString(2));
        }
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(baseDir + "/test.csv");
    }

    private String randomData(final Random random) {

        if (random.nextInt(10) == 1) { return null; }
        final int len = random.nextInt(5);
        final StringBuilder buff = new StringBuilder();
        final String chars = "\\\'\",\r\n\t ;.-123456|#";
        for (int i = 0; i < len; i++) {
            buff.append(chars.charAt(random.nextInt(chars.length())));
        }
        return buff.toString();
    }

    private void testEmptyFieldDelimiter() throws Exception {

        final File f = new File(baseDir + "/test.csv");
        f.delete();
        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + baseDir + "/test.csv', 'select 1 id, ''Hello'' name', null, '|', '', null, null, chr(10))");
        final FileReader reader = new FileReader(baseDir + "/test.csv");
        String text = IOUtils.readStringAndClose(reader, -1).trim();
        text = StringUtils.replaceAll(text, "\n", " ");
        assertEquals("ID|NAME 1|Hello", text);
        final ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, null, '|', '')");
        final ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 2);
        assertEquals(meta.getColumnLabel(1), "ID");
        assertEquals(meta.getColumnLabel(2), "NAME");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "1");
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(baseDir + "/test.csv");
    }

    private void testFieldDelimiter() throws Exception {

        final File f = new File(baseDir + "/test.csv");
        f.delete();
        final RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.write("'A'; 'B'\n\'It\\'s nice\'; '\nHello\\*\n'".getBytes());
        file.close();
        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, null, ';', '''', '\\')");
        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 2);
        assertEquals(meta.getColumnLabel(1), "A");
        assertEquals(meta.getColumnLabel(2), "B");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "It's nice");
        assertEquals(rs.getString(2), "\nHello*\n");
        assertFalse(rs.next());
        stat.execute("call csvwrite('" + baseDir + "/test2.csv', 'select * from csvread(''" + baseDir + "/test.csv'', null, null, '';'', '''''''', ''\\'')', null, '+', '*', '#')");
        rs = stat.executeQuery("select * from csvread('" + baseDir + "/test2.csv', null, null, '+', '*', '#')");
        meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 2);
        assertEquals(meta.getColumnLabel(1), "A");
        assertEquals(meta.getColumnLabel(2), "B");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "It's nice");
        assertEquals(rs.getString(2), "\nHello*\n");
        assertFalse(rs.next());
        conn.close();
        FileUtils.delete(baseDir + "/test.csv");
        FileUtils.delete(baseDir + "/test2.csv");
    }

    private void testPipe() throws SQLException, IOException {

        deleteDb("csv");
        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + baseDir + "/test.csv', 'select 1 id, ''Hello'' name', 'utf-8', '|')");
        final ResultSet rs = stat.executeQuery("select * from csvread('" + baseDir + "/test.csv', null, 'utf-8', '|')");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());
        new File(baseDir + "/test.csv").delete();

        // PreparedStatement prep = conn.prepareStatement("select * from
        // csvread(?, null, ?, ?)");
        // prep.setString(1, BASE_DIR+"/test.csv");
        // prep.setString(2, "utf-8");
        // prep.setString(3, "|");
        // rs = prep.executeQuery();

        conn.close();
        FileUtils.delete(baseDir + "/test.csv");
    }

    private void testAsTable() throws SQLException, IOException {

        deleteDb("csv");
        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("call csvwrite('" + baseDir + "/test.csv', 'select 1 id, ''Hello'' name')");
        ResultSet rs = stat.executeQuery("select name from csvread('" + baseDir + "/test.csv')");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "Hello");
        assertFalse(rs.next());
        rs = stat.executeQuery("call csvread('" + baseDir + "/test.csv')");
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        assertFalse(rs.next());
        new File(baseDir + "/test.csv").delete();
        conn.close();
    }

    private void testRead() throws Exception {

        final File f = new File(baseDir + "/test.csv");
        f.delete();
        final RandomAccessFile file = new RandomAccessFile(f, "rw");
        file.write("a,b,c,d\n201,-2,0,18\n, \"abc\"\"\" ,,\"\"\n 1 ,2 , 3, 4 \n5, 6, 7, 8".getBytes());
        file.close();
        final ResultSet rs = Csv.getInstance().read(baseDir + "/test.csv", null, "UTF8");
        final ResultSetMetaData meta = rs.getMetaData();
        assertEquals(meta.getColumnCount(), 4);
        assertEquals(meta.getColumnLabel(1), "a");
        assertEquals(meta.getColumnLabel(2), "b");
        assertEquals(meta.getColumnLabel(3), "c");
        assertEquals(meta.getColumnLabel(4), "d");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "201");
        assertEquals(rs.getString(2), "-2");
        assertEquals(rs.getString(3), "0");
        assertEquals(rs.getString(4), "18");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), null);
        assertEquals(rs.getString(2), "abc\"");
        assertEquals(rs.getString(3), null);
        assertEquals(rs.getString(4), "");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "1");
        assertEquals(rs.getString(2), "2");
        assertEquals(rs.getString(3), "3");
        assertEquals(rs.getString(4), "4");
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "5");
        assertEquals(rs.getString(2), "6");
        assertEquals(rs.getString(3), "7");
        assertEquals(rs.getString(4), "8");
        assertFalse(rs.next());

        // a,b,c,d
        // 201,-2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        // 201,2,0,18
        FileUtils.delete(baseDir + "/test.csv");
    }

    private void testWriteRead() throws SQLException, IOException {

        deleteDb("csv");

        final Connection conn = getConnection("csv");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID IDENTITY, NAME VARCHAR)");
        final int len = 100;
        for (int i = 0; i < len; i++) {
            stat.execute("INSERT INTO TEST(NAME) VALUES('Ruebezahl')");
        }
        Csv.getInstance().write(conn, baseDir + "/testRW.csv", "SELECT * FROM TEST", "UTF8");
        final ResultSet rs = Csv.getInstance().read(baseDir + "/testRW.csv", null, "UTF8");
        // stat.execute("CREATE ALIAS CSVREAD FOR \"org.h2.tools.Csv.read\"");
        final ResultSetMetaData meta = rs.getMetaData();
        assertEquals(2, meta.getColumnCount());
        for (int i = 0; i < len; i++) {
            rs.next();
            assertEquals(rs.getString("ID"), "" + (i + 1));
            assertEquals(rs.getString("NAME"), "Ruebezahl");
        }
        assertFalse(rs.next());
        rs.close();
        conn.close();
        FileUtils.delete(baseDir + "/testRW.csv");
    }

}
