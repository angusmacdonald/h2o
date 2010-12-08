/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.store.fs.FileSystem;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseID;
import org.h2o.run.AllTests;
import org.h2o.util.H2OPropertiesWrapper;

/**
 * The base class for all tests.
 */
public abstract class TestBase {

    /**
     * The base directory to write test databases.
     */
    protected static String baseDir = getTestDir("");

    private static final String BASE_TEST_DIR = "data";

    /**
     * The test configuration.
     */
    public TestAll config;

    /**
     * The time when the test was started.
     */
    protected long start;

    /**
     * Get the test directory for this test.
     * 
     * @param name
     *            the directory name suffix
     * @return the test directory
     */
    public static String getTestDir(final String name) {

        return BASE_TEST_DIR + "/test" + name;
    }

    /**
     * Start the TCP server if enabled in the configuration.
     */
    protected void startServerIfRequired() throws SQLException {

        config.beforeTest();
    }

    /**
     * Initialize the test configuration using the default settings.
     * 
     * @return itself
     */
    public TestBase init() {

        baseDir = getTestDir("");
        config = new TestAll();
        return this;
    }

    /**
     * Initialize the test configuration.
     * 
     * @param conf
     *            the configuration
     * @return itself
     */
    public TestBase init(final TestAll conf) throws Exception {

        baseDir = getTestDir("");
        config = conf;
        return this;
    }

    /**
     * Run a test case using the given seed value.
     * 
     * @param seed
     *            the random seed value
     */
    public void testCase(final int seed) throws Exception {

        // do nothing
    }

    /**
     * This method is initializes the test, runs the test by calling the test() method, and prints status information. It also catches
     * exceptions so that the tests can continue.
     * 
     * @param conf
     *            the test configuration
     */
    public void runTest(final TestAll conf) {

        try {
            init(conf);
            start = System.currentTimeMillis();
            test();
            println("");
        }
        catch (final Throwable e) {
            println("FAIL " + e.toString());
            logError("FAIL " + e.toString(), e);
            if (config.stopOnError) { throw new Error("ERROR"); }
        }
        finally {
            try {
                FileSystem.getInstance("memFS:").deleteRecursive("memFS:");
                FileSystem.getInstance("memLZF:").deleteRecursive("memLZF:");
            }
            catch (final SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Open a database connection in admin mode. The default user name and password is used.
     * 
     * @param name the database name
     * @return the connection
     * @throws IOException 
     */
    public Connection getConnection(final String name) throws SQLException, IOException {

        final String databaseURL = getURL(name, true);
        setUpDescriptorFiles(databaseURL);
        return getConnectionInternal(databaseURL, getUser(), getPassword());
    }

    public static void setUpDescriptorFiles(final String url) throws IOException {

        final H2OPropertiesWrapper properties = H2OPropertiesWrapper.getWrapper(DatabaseID.parseURL(url));
        properties.createNewFile();
        properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
        properties.setProperty("databaseName", "testDB");
        properties.setProperty("chordPort", "" + 50000);
        properties.saveAndClose();
    }

    /**
     * Open a database connection.
     * 
     * @param name the database name
     * @param user the user name to use
     * @param password the password to use
     * @return the connection
     */
    protected Connection getConnection(final String name, final String user, final String password) throws SQLException {

        return getConnectionInternal(getURL(name, false), user, password);
    }

    /**
     * Get the password to use to login for the given user password. The file password is added if required.
     * 
     * @param userPassword the password of this user
     * @return the login password
     */
    protected String getPassword(final String userPassword) {

        return config == null || config.cipher == null ? userPassword : "filePassword " + userPassword;
    }

    /**
     * Get the login password. This is usually the user password. If file encryption is used it is combined with the file password.
     * 
     * @return the login password
     */
    protected String getPassword() {

        return getPassword("123");
    }

    private void deleteIndexFiles(String name) {

        if (name.indexOf(";") > 0) {
            name = name.substring(0, name.indexOf(';'));
        }
        name += ".index.db";
        if (new File(name).canWrite()) {
            new File(name).delete();
        }
    }

    /**
     * Get the database URL for the given database name using the current configuration options.
     * 
     * @param name
     *            the database name
     * @param admin
     *            true if the current user is an admin
     * @return the database URL
     */
    protected String getURL(String name, final boolean admin) {

        String url;
        if (name.startsWith("jdbc:")) { return name; }
        if (config.memory) {
            name = "mem:" + name;
        }
        else {
            if (!name.startsWith("memFS:") && !name.startsWith(baseDir + "/")) {
                name = baseDir + "/" + name;
            }
            if (config.deleteIndex) {
                deleteIndexFiles(name);
            }
        }
        if (config.networked) {
            if (config.ssl) {
                url = "ssl://localhost:9192/" + name;
            }
            else {
                url = "tcp://localhost:9192/" + name;
            }
        }
        else {
            url = name;
        }
        if (!config.memory) {
            if (admin) {
                url += ";LOG=" + config.logMode;
            }
            if (config.smallLog && admin) {
                url += ";MAX_LOG_SIZE=1";
            }
        }
        if (config.traceSystemOut) {
            url += ";TRACE_LEVEL_SYSTEM_OUT=2";
        }
        if (config.traceLevelFile > 0 && admin) {
            if (url.indexOf("TRACE_LEVEL_FILE=") < 0) {
                url += ";TRACE_LEVEL_FILE=" + config.traceLevelFile;
            }
        }
        if (config.throttle > 0) {
            url += ";THROTTLE=" + config.throttle;
        }
        if (url.indexOf("LOCK_TIMEOUT=") < 0) {
            url += ";LOCK_TIMEOUT=50";
        }
        if (config.diskUndo && admin) {
            url += ";MAX_MEMORY_UNDO=3";
        }
        if (config.big && admin) {
            // force operations to disk
            url += ";MAX_OPERATION_MEMORY=1";
        }
        if (config.mvcc && url.indexOf("MVCC=") < 0) {
            url += ";MVCC=TRUE";
        }
        if (config.cache2Q) {
            url += ";CACHE_TYPE=TQ";
        }
        if (config.diskResult && admin) {
            url += ";MAX_MEMORY_ROWS=100;CACHE_SIZE=0";
        }
        if (config.cipher != null) {
            url += ";CIPHER=" + config.cipher;
        }
        return "jdbc:h2:" + url;
    }

    private Connection getConnectionInternal(final String url, final String user, final String password) throws SQLException {

        org.h2.Driver.load();
        // url += ";DEFAULT_TABLE_TYPE=1";
        // Class.forName("org.hsqldb.jdbcDriver");
        // return DriverManager.getConnection("jdbc:hsqldb:" + name, "sa", "");
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Get the small or the big value depending on the configuration.
     * 
     * @param small
     *            the value to return if the current test mode is 'small'
     * @param big
     *            the value to return if the current test mode is 'big'
     * @return small or big, depending on the configuration
     */
    protected int getSize(final int small, final int big) {

        return config.endless ? Integer.MAX_VALUE : config.big ? big : small;
    }

    protected String getUser() {

        return "sa";
    }

    /**
     * Write a message to system out if trace is enabled.
     * 
     * @param x
     *            the value to write
     */
    protected void trace(final int x) {

        trace("" + x);
    }

    /**
     * Write a message to system out if trace is enabled.
     * 
     * @param s
     *            the message to write
     */
    public void trace(final String s) {

        if (config.traceTest) {
            println(s);
        }
    }

    /**
     * Print how much memory is currently used.
     */
    protected void traceMemory() {

        if (config.traceTest) {
            trace("mem=" + getMemoryUsed());
        }
    }

    /**
     * Print the currently used memory, the message and the given time in milliseconds.
     * 
     * @param s
     *            the message
     * @param time
     *            the time in millis
     */
    public void printTimeMemory(final String s, final long time) {

        if (config.big) {
            println(getMemoryUsed() + " MB: " + s + " ms: " + time);
        }
    }

    /**
     * Get the number of megabytes heap memory in use.
     * 
     * @return the used megabytes
     */
    public static int getMemoryUsed() {

        final Runtime rt = Runtime.getRuntime();
        long memory = Long.MAX_VALUE;
        for (int i = 0; i < 8; i++) {
            rt.gc();
            final long memNow = rt.totalMemory() - rt.freeMemory();
            if (memNow >= memory) {
                break;
            }
            memory = memNow;
        }
        final int mb = (int) (memory / 1024 / 1024);
        return mb;
    }

    /**
     * Called if the test reached a point that was not expected.
     * 
     * @throws AssertionError
     *             always throws an AssertionError
     */
    protected void fail() {

        fail("Unexpected success");
    }

    /**
     * Called if the test reached a point that was not expected.
     * 
     * @param string
     *            the error message
     * @throws AssertionError
     *             always throws an AssertionError
     */
    protected void fail(final String string) {

        println(string);
        throw new AssertionError(string);
    }

    /**
     * Log an error message.
     * 
     * @param s
     *            the message
     * @param e
     *            the exception
     */
    public static void logError(final String s, Throwable e) {

        if (e == null) {
            e = new Exception(s);
        }
        System.out.println("ERROR: " + s + " " + e.toString() + " ------------------------------");
        System.out.flush();
        e.printStackTrace();
        try {
            final TraceSystem ts = new TraceSystem(null, false);
            final FileLock lock = new FileLock(ts, "error.lock", 1000);
            lock.lock(FileLock.LOCK_FILE);
            final FileWriter fw = new FileWriter("ERROR.txt", true);
            final PrintWriter pw = new PrintWriter(fw);
            e.printStackTrace(pw);
            pw.close();
            fw.close();
            lock.unlock();
        }
        catch (final Throwable t) {
            t.printStackTrace();
        }
        System.err.flush();
    }

    /**
     * Print a message to system out.
     * 
     * @param s
     *            the message
     */
    protected void println(final String s) {

        final long time = System.currentTimeMillis() - start;
        printlnWithTime(time, getClass().getName() + " " + s);
    }

    /**
     * Print a message, prepended with the specified time in milliseconds.
     * 
     * @param millis
     *            the time in milliseconds
     * @param s
     *            the message
     */
    static void printlnWithTime(final long millis, final String s) {

        System.out.println(formatTime(millis) + " " + s);
    }

    /**
     * Print the current time and a message to system out.
     * 
     * @param s
     *            the message
     */
    protected void printTime(final String s) {

        final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
        println(dateFormat.format(new java.util.Date()) + " " + s);
    }

    /**
     * Format the time in the format hh:mm:ss.1234 where 1234 is milliseconds.
     * 
     * @param millis
     *            the time in milliseconds
     * @return the formatted time
     */
    static String formatTime(final long millis) {

        String s = new java.sql.Time(java.sql.Time.valueOf("0:0:0").getTime() + millis).toString() + "." + ("" + (1000 + millis % 1000)).substring(1);
        if (s.startsWith("00:")) {
            s = s.substring(3);
        }
        return s;
    }

    /**
     * Delete all database files for this database.
     * 
     * @param name
     *            the database name
     */
    protected void deleteDb(final String name) throws SQLException {

        DeleteDbFiles.execute(baseDir, name, true);
    }

    /**
     * Delete all database files for a database.
     * 
     * @param dir
     *            the directory where the database files are located
     * @param name
     *            the database name
     */
    protected void deleteDb(final String dir, final String name) throws SQLException {

        DeleteDbFiles.execute(dir, name, true);
    }

    /**
     * This method will be called by the test framework.
     * 
     * @throws Exception
     *             if an exception in the test occurs
     */
    public abstract void test() throws Exception;

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param message
     *            the message to print in case of error
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    public void assertEquals(final String message, final int expected, final int actual) {

        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual + " message: " + message);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    public void assertEquals(final int expected, final int actual) {

        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final byte[] expected, final byte[] actual) {

        assertEquals("length", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual[i]) {
                fail("[" + i + "]: expected: " + (int) expected[i] + " actual: " + (int) actual[i]);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(String expected, String actual) {

        if (expected == null && actual == null) {
            return;
        }
        else if (expected == null || actual == null) {
            fail("Expected: " + expected + " Actual: " + actual);
        }
        if (!expected.equals(actual)) {
            for (int i = 0; i < expected.length(); i++) {
                final String s = expected.substring(0, i);
                if (!actual.startsWith(s)) {
                    expected = expected.substring(0, i) + "<*>" + expected.substring(i);
                    break;
                }
            }
            final int al = expected.length();
            final int bl = actual.length();
            if (al > 4000) {
                expected = expected.substring(0, 4000);
            }
            if (bl > 4000) {
                actual = actual.substring(0, 4000);
            }
            fail("Expected: " + expected + " (" + al + ") actual: " + actual + " (" + bl + ")");
        }
    }

    /**
     * Check if the first value is larger or equal than the second value, and if not throw an exception.
     * 
     * @param a
     *            the first value
     * @param b
     *            the second value (must be smaller than the first value)
     * @throws AssertionError
     *             if the first value is smaller
     */
    protected void assertSmaller(final long a, final long b) {

        if (a >= b) {
            fail("a: " + a + " is not smaller than b: " + b);
        }
    }

    /**
     * Check that a result contains the given substring.
     * 
     * @param result
     *            the result value
     * @param contains
     *            the term that should appear in the result
     * @throws AssertionError
     *             if the term was not found
     */
    protected void assertContains(final String result, final String contains) {

        if (result.indexOf(contains) < 0) {
            fail(result + " does not contain: " + contains);
        }
    }

    /**
     * Check that a text starts with the expected characters..
     * 
     * @param text
     *            the text
     * @param expectedStart
     *            the expected prefix
     * @throws AssertionError
     *             if the text does not start with the expected characters
     */
    protected void assertStartsWith(final String text, final String expectedStart) {

        if (!text.startsWith(expectedStart)) {
            fail(text + " does not start with: " + expectedStart);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final long expected, final long actual) {

        if (expected != actual) {
            fail("Expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final double expected, final double actual) {

        if (expected != actual) {
            if (Double.isNaN(expected) && Double.isNaN(actual)) {
                // if both a NaN, then there is no error
            }
            else {
                fail("Expected: " + expected + " actual: " + actual);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final float expected, final float actual) {

        if (expected != actual) {
            if (Float.isNaN(expected) && Float.isNaN(actual)) {
                // if both a NaN, then there is no error
            }
            else {
                fail("Expected: " + expected + " actual: " + actual);
            }
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final boolean expected, final boolean actual) {

        if (expected != actual) {
            fail("Boolean expected: " + expected + " actual: " + actual);
        }
    }

    /**
     * Check that the passed boolean is true.
     * 
     * @param condition
     *            the condition
     * @throws AssertionError
     *             if the condition is false
     */
    protected void assertTrue(final boolean condition) {

        assertTrue("Expected: true got: false", condition);
    }

    /**
     * Check that the passed boolean is true.
     * 
     * @param message
     *            the message to print if the condition is false
     * @param condition
     *            the condition
     * @throws AssertionError
     *             if the condition is false
     */
    protected void assertTrue(final String message, final boolean condition) {

        if (!condition) {
            fail(message);
        }
    }

    /**
     * Check that the passed boolean is false.
     * 
     * @param value
     *            the condition
     * @throws AssertionError
     *             if the condition is true
     */
    protected void assertFalse(final boolean value) {

        assertFalse("Expected: false got: true", value);
    }

    /**
     * Check that the passed boolean is false.
     * 
     * @param message
     *            the message to print if the condition is false
     * @param value
     *            the condition
     * @throws AssertionError
     *             if the condition is true
     */
    protected void assertFalse(final String message, final boolean value) {

        if (value) {
            fail(message);
        }
    }

    /**
     * Check that the result set row count matches.
     * 
     * @param rs
     *            the result set
     * @param expected
     *            the number of expected rows
     * @throws AssertionError
     *             if a different number of rows have been found
     */
    protected void assertResultRowCount(final ResultSet rs, final int expected) throws SQLException {

        int i = 0;
        while (rs.next()) {
            i++;
        }
        assertEquals(i, expected);
    }

    /**
     * Check that the result set of a query is exactly this value.
     * 
     * @param stat
     *            the statement
     * @param sql
     *            the SQL statement to execute
     * @param expected
     *            the expected result value
     * @throws AssertionError
     *             if a different result value was returned
     */
    protected void assertSingleValue(final Statement stat, final String sql, final int expected) throws SQLException {

        final ResultSet rs = stat.executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertFalse(rs.next());
    }

    /**
     * Check that the result set of a query is exactly this value.
     * 
     * @param stat
     *            the statement
     * @param sql
     *            the SQL statement to execute
     * @param expected
     *            the expected result value
     * @throws AssertionError
     *             if a different result value was returned
     */
    protected void assertResult(final Statement stat, final String sql, final String expected) throws SQLException {

        final ResultSet rs = stat.executeQuery(sql);
        if (rs.next()) {
            final String actual = rs.getString(1);
            assertEquals(expected, actual);
        }
        else {
            assertEquals(null, expected);
        }
    }

    /**
     * Check if the result set meta data is correct.
     * 
     * @param rs
     *            the result set
     * @param columnCount
     *            the expected column count
     * @param labels
     *            the expected column labels
     * @param datatypes
     *            the expected data types
     * @param precision
     *            the expected precisions
     * @param scale
     *            the expected scales
     */
    protected void assertResultSetMeta(final ResultSet rs, final int columnCount, final String[] labels, final int[] datatypes, final int[] precision, final int[] scale) throws SQLException {

        final ResultSetMetaData meta = rs.getMetaData();
        final int cc = meta.getColumnCount();
        if (cc != columnCount) {
            fail("result set contains " + cc + " columns not " + columnCount);
        }
        for (int i = 0; i < columnCount; i++) {
            if (labels != null) {
                final String l = meta.getColumnLabel(i + 1);
                if (!labels[i].equals(l)) {
                    fail("column label " + i + " is " + l + " not " + labels[i]);
                }
            }
            if (datatypes != null) {
                final int t = meta.getColumnType(i + 1);
                if (datatypes[i] != t) {
                    fail("column datatype " + i + " is " + t + " not " + datatypes[i] + " (prec=" + meta.getPrecision(i + 1) + " scale=" + meta.getScale(i + 1) + ")");
                }
                final String typeName = meta.getColumnTypeName(i + 1);
                final String className = meta.getColumnClassName(i + 1);
                switch (t) {
                    case Types.INTEGER:
                        assertEquals(typeName, "INTEGER");
                        assertEquals(className, "java.lang.Integer");
                        break;
                    case Types.VARCHAR:
                        assertEquals(typeName, "VARCHAR");
                        assertEquals(className, "java.lang.String");
                        break;
                    case Types.SMALLINT:
                        assertEquals(typeName, "SMALLINT");
                        assertEquals(className, "java.lang.Short");
                        break;
                    case Types.TIMESTAMP:
                        assertEquals(typeName, "TIMESTAMP");
                        assertEquals(className, "java.sql.Timestamp");
                        break;
                    case Types.DECIMAL:
                        assertEquals(typeName, "DECIMAL");
                        assertEquals(className, "java.math.BigDecimal");
                        break;
                    default:
                }
            }
            if (precision != null) {
                final int p = meta.getPrecision(i + 1);
                if (precision[i] != p) {
                    fail("column precision " + i + " is " + p + " not " + precision[i]);
                }
            }
            if (scale != null) {
                final int s = meta.getScale(i + 1);
                if (scale[i] != s) {
                    fail("column scale " + i + " is " + s + " not " + scale[i]);
                }
            }

        }
    }

    /**
     * Check if a result set contains the expected data. The sort order is significant
     * 
     * @param rs
     *            the result set
     * @param data
     *            the expected data
     * @throws AssertionError
     *             if there is a mismatch
     */
    protected void assertResultSetOrdered(final ResultSet rs, final String[][] data) throws SQLException {

        assertResultSet(true, rs, data);
    }

    /**
     * Check if a result set contains the expected data. The sort order is not significant
     * 
     * @param rs
     *            the result set
     * @param data
     *            the expected data
     * @throws AssertionError
     *             if there is a mismatch
     */
    // void assertResultSetUnordered(ResultSet rs, String[][] data) {
    // assertResultSet(false, rs, data);
    // }

    /**
     * Check if a result set contains the expected data.
     * 
     * @param ordered
     *            if the sort order is significant
     * @param rs
     *            the result set
     * @param data
     *            the expected data
     * @throws AssertionError
     *             if there is a mismatch
     */
    private void assertResultSet(final boolean ordered, final ResultSet rs, final String[][] data) throws SQLException {

        final int len = rs.getMetaData().getColumnCount();
        final int rows = data.length;
        if (rows == 0) {
            // special case: no rows
            if (rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:0");
            }
        }
        final int len2 = data[0].length;
        if (len < len2) {
            fail("testResultSet expected columnCount:" + len2 + " got:" + len);
        }
        for (int i = 0; i < rows; i++) {
            if (!rs.next()) {
                fail("testResultSet expected rowCount:" + rows + " got:" + i);
            }
            final String[] row = getData(rs, len);
            if (ordered) {
                final String[] good = data[i];
                if (!testRow(good, row, good.length)) {
                    fail("testResultSet row not equal, got:\n" + formatRow(row) + "\n" + formatRow(good));
                }
            }
            else {
                boolean found = false;
                for (int j = 0; j < rows; j++) {
                    final String[] good = data[i];
                    if (testRow(good, row, good.length)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    fail("testResultSet no match for row:" + formatRow(row));
                }
            }
        }
        if (rs.next()) {
            final String[] row = getData(rs, len);
            fail("testResultSet expected rowcount:" + rows + " got:>=" + (rows + 1) + " data:" + formatRow(row));
        }
    }

    private boolean testRow(final String[] a, final String[] b, final int len) {

        for (int i = 0; i < len; i++) {
            final String sa = a[i];
            final String sb = b[i];
            if (sa == null || sb == null) {
                if (sa != sb) { return false; }
            }
            else {
                if (!sa.equals(sb)) { return false; }
            }
        }
        return true;
    }

    private String[] getData(final ResultSet rs, final int len) throws SQLException {

        final String[] data = new String[len];
        for (int i = 0; i < len; i++) {
            data[i] = rs.getString(i + 1);
            // just check if it works
            rs.getObject(i + 1);
        }
        return data;
    }

    private String formatRow(final String[] row) {

        String sb = "";
        for (final String element : row) {
            sb += "{" + element + "}";
        }
        return "{" + sb + "}";
    }

    /**
     * Simulate a database crash. This method will also close the database files, but the files are in a state as the power was switched
     * off. It doesn't throw an exception.
     * 
     * @param conn
     *            the database connection
     */
    protected void crash(final Connection conn) throws SQLException {

        ((JdbcConnection) conn).setPowerOffCount(1);
        try {
            conn.createStatement().execute("SET WRITE_DELAY 0");
            conn.createStatement().execute("CREATE TABLE TEST_A(ID INT)");
            fail("should be crashed already");
        }
        catch (final SQLException e) {
            // expected
        }
        try {
            conn.close();
        }
        catch (final SQLException e) {
            // ignore
        }
    }

    /**
     * Read a string from the reader. This method reads until end of file.
     * 
     * @param reader
     *            the reader
     * @return the string read
     */
    protected String readString(final Reader reader) {

        if (reader == null) { return null; }
        final StringBuilder buffer = new StringBuilder();
        try {
            while (true) {
                final int c = reader.read();
                if (c == -1) {
                    break;
                }
                buffer.append((char) c);
            }
            return buffer.toString();
        }
        catch (final Exception e) {
            assertTrue(false);
            return null;
        }
    }

    /**
     * Check that a given exception is not an unexpected 'general error' exception.
     * 
     * @param e
     *            the error
     */
    protected void assertKnownException(final SQLException e) {

        assertKnownException("", e);
    }

    /**
     * Check that a given exception is not an unexpected 'general error' exception.
     * 
     * @param message
     *            the message
     * @param e
     *            the exception
     */
    protected void assertKnownException(final String message, final SQLException e) {

        if (e != null && e.getSQLState().startsWith("HY000")) {
            TestBase.logError("Unexpected General error " + message, e);
        }
    }

    /**
     * Check if two values are equal, and if not throw an exception.
     * 
     * @param expected
     *            the expected value
     * @param actual
     *            the actual value
     * @throws AssertionError
     *             if the values are not equal
     */
    protected void assertEquals(final Integer expected, final Integer actual) {

        if (expected == null || actual == null) {
            assertTrue(expected == actual);
        }
        else {
            assertEquals(expected.intValue(), actual.intValue());
        }
    }

    /**
     * Check if two databases contain the same met data.
     * 
     * @param stat1
     *            the connection to the first database
     * @param stat2
     *            the connection to the second database
     * @throws AssertionError
     *             if the databases don't match
     */
    protected void assertEqualDatabases(final Statement stat1, final Statement stat2) throws SQLException {

        final ResultSet rs1 = stat1.executeQuery("SCRIPT NOPASSWORDS");
        final ResultSet rs2 = stat2.executeQuery("SCRIPT NOPASSWORDS");
        final ArrayList list1 = new ArrayList();
        final ArrayList list2 = new ArrayList();
        while (rs1.next()) {
            final String s1 = rs1.getString(1);
            list1.add(s1);
            if (!rs2.next()) {
                fail("expected: " + s1);
            }
            final String s2 = rs2.getString(1);
            list2.add(s2);
        }
        for (int i = 0; i < list1.size(); i++) {
            final String s = (String) list1.get(i);
            if (!list2.remove(s)) {
                fail("not found: " + s);
            }
        }
        assertEquals(list2.size(), 0);
        assertFalse(rs2.next());
    }

    /**
     * Create a new object of the calling class.
     * 
     * @return the new test
     */
    public static TestBase createCaller() {

        final String className = new Exception().getStackTrace()[1].getClassName();
        org.h2.Driver.load();
        try {
            return (TestBase) Class.forName(className).newInstance();
        }
        catch (final Exception e) {
            throw new RuntimeException("Can not create object " + className, e);
        }
    }

}
