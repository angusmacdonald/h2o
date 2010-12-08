/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseID;
import org.h2o.run.AllTests;
import org.h2o.util.LocalH2OProperties;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * The base class for all tests.
 */
public abstract class H2TestBase {

    private static int port = 50000;

    /**
     * The base directory to write test databases.
     */
    protected String baseDir = getTestDir("");

    protected static final String BASE_TEST_DIR = "data";

    protected static final long SHUTDOWN_CHECK_DELAY = 2000;

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
    public H2TestBase init() {

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
    public H2TestBase init(final TestAll conf) throws Exception {

        baseDir = getTestDir("");
        config = conf;
        return this;
    }

    /**
     * Open a database connection in admin mode. The default user name and password is used.
     * 
     * @param name
     *            the database name
     * @return the connection
     * @throws IOException 
     */
    public Connection getConnection(final String name) throws SQLException, IOException {

        final String databaseURL = getURL(name, true);
        setUpDescriptorFiles(databaseURL);
        return getConnectionInternal(databaseURL, getUser(), getPassword());
    }

    private static void setUpDescriptorFiles(final String url) throws IOException {

        final LocalH2OProperties properties = new LocalH2OProperties(DatabaseID.parseURL(url));

        properties.createNewFile();
        properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
        properties.setProperty("databaseName", "testDB");
        properties.setProperty("chordPort", "" + ++port);
        properties.saveAndClose();
    }

    /**
     * Open a database connection.
     * 
     * @param name
     *            the database name
     * @param user
     *            the user name to use
     * @param password
     *            the password to use
     * @return the connection
     */
    protected Connection getConnection(final String name, final String user, final String password) throws SQLException {

        return getConnectionInternal(getURL(name, false), user, password);
    }

    /**
     * Get the password to use to login for the given user password. The file password is added if required.
     * 
     * @param userPassword
     *            the password of this user
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
            final boolean successful = new File(name).delete();

            if (!successful) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Failed to delete file.");
            }
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
                url = "tcp://localhost:9402/" + name;
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

        final long memory = Long.MAX_VALUE;

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
     * /** Check if a result set contains the expected data. The sort order is significant
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

        final StringBuilder sc = new StringBuilder();
        sc.append("{");
        for (final String element : row) {
            sc.append("{");
            sc.append(element);
            sc.append("}");
        }

        sc.append("}");
        return sc.toString();
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
            H2TestBase.logError("Unexpected General error " + message, e);
        }
    }

    /**
     * Create a new object of the calling class.
     * 
     * @return the new test
     */
    public static H2TestBase createCaller() {

        final String className = new Exception().getStackTrace()[1].getClassName();

        try {
            return (H2TestBase) Class.forName(className).newInstance();
        }
        catch (final Exception e) {
            throw new RuntimeException("Can not create object " + className, e);
        }
    }
}
