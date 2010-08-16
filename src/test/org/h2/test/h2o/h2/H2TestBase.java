/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.h2o.h2;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.TraceSystem;
import org.h2.store.FileLock;
import org.h2.test.TestAll;
import org.h2.test.h2o.AllTests;
import org.h2.tools.DeleteDbFiles;
import org.h2o.db.id.DatabaseURL;
import org.h2o.util.LocalH2OProperties;

/**
 * The base class for all tests.
 */
public abstract class H2TestBase {


	private static int port = 50000;
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

	static {
		Constants.IS_H2O = true;
	}

	/**
	 * Get the test directory for this test.
	 *
	 * @param name the directory name suffix
	 * @return the test directory
	 */
	public static String getTestDir(String name) {
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
		this.config = new TestAll();
		Constants.IS_H2O = true;
		return this;
	}

	/**
	 * Initialize the test configuration.
	 *
	 * @param conf the configuration
	 * @return itself
	 */
	public H2TestBase init(TestAll conf) throws Exception {
		baseDir = getTestDir("");
		this.config = conf;
		return this;
	}

	/**
	 * Run a test case using the given seed value.
	 *
	 * @param seed the random seed value
	 */
	public void testCase(int seed) throws Exception {
		// do nothing
	}


	/**
	 * Open a database connection in admin mode. The default user name and
	 * password is used.
	 *
	 * @param name the database name
	 * @return the connection
	 */
	public Connection getConnection(String name) throws SQLException {
		String databaseURL = getURL(name, true);
		setUpDescriptorFiles(databaseURL);
		return getConnectionInternal(databaseURL, getUser(), getPassword());
	}

	public static void setUpDescriptorFiles(String url) {
		LocalH2OProperties properties = new LocalH2OProperties(DatabaseURL.parseURL(url));
		properties.createNewFile();
		properties.setProperty("descriptor", AllTests.TEST_DESCRIPTOR_FILE);
		properties.setProperty("databaseName", "testDB");
		properties.setProperty("chordPort", "" + ++port);
		properties.saveAndClose();
		//
		//		LocatorServer ls = new LocatorServer(29999, "junitLocator");
		//		ls.createNewLocatorFile();
		//		ls.start();
	}

	/**
	 * Open a database connection.
	 *
	 * @param name the database name
	 * @param user the user name to use
	 * @param password the password to use
	 * @return the connection
	 */
	protected Connection getConnection(String name, String user, String password) throws SQLException {
		return getConnectionInternal(getURL(name, false), user, password);
	}

	/**
	 * Get the password to use to login for the given user password. The file
	 * password is added if required.
	 *
	 * @param userPassword the password of this user
	 * @return the login password
	 */
	protected String getPassword(String userPassword) {
		return config == null || config.cipher == null ? userPassword : "filePassword " + userPassword;
	}

	/**
	 * Get the login password. This is usually the user password. If file
	 * encryption is used it is combined with the file password.
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
	 * Get the database URL for the given database name using the current
	 * configuration options.
	 *
	 * @param name the database name
	 * @param admin true if the current user is an admin
	 * @return the database URL
	 */
	protected String getURL(String name, boolean admin) {
		String url;
		if (name.startsWith("jdbc:")) {
			return name;
		}
		if (config.memory) {
			name = "mem:" + name;
		} else {
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
			} else {
				url = "tcp://localhost:9402/" + name;
			}
		} else {
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

	private Connection getConnectionInternal(String url, String user, String password) throws SQLException {
		org.h2.Driver.load();
		// url += ";DEFAULT_TABLE_TYPE=1";
		// Class.forName("org.hsqldb.jdbcDriver");
		// return DriverManager.getConnection("jdbc:hsqldb:" + name, "sa", "");
		return DriverManager.getConnection(url, user, password);
	}

	/**
	 * Get the small or the big value depending on the configuration.
	 *
	 * @param small the value to return if the current test mode is 'small'
	 * @param big the value to return if the current test mode is 'big'
	 * @return small or big, depending on the configuration
	 */
	protected int getSize(int small, int big) {
		return config.endless ? Integer.MAX_VALUE : config.big ? big : small;
	}

	protected String getUser() {
		return "sa";
	}

	/**
	 * Write a message to system out if trace is enabled.
	 *
	 * @param x the value to write
	 */
	protected void trace(int x) {
		trace("" + x);
	}

	/**
	 * Write a message to system out if trace is enabled.
	 *
	 * @param s the message to write
	 */
	public void trace(String s) {
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
	 * Print the currently used memory, the message and the given time in
	 * milliseconds.
	 *
	 * @param s the message
	 * @param time the time in millis
	 */
	public void printTimeMemory(String s, long time) {
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
		Runtime rt = Runtime.getRuntime();
		long memory = Long.MAX_VALUE;
		for (int i = 0; i < 8; i++) {
			rt.gc();
			long memNow = rt.totalMemory() - rt.freeMemory();
			if (memNow >= memory) {
				break;
			}
			memory = memNow;
		}
		int mb = (int) (memory / 1024 / 1024);
		return mb;
	}

	/**
	 * Called if the test reached a point that was not expected.
	 *
	 * @throws AssertionError always throws an AssertionError
	 */
	protected void fail() {
		fail("Unexpected success");
	}

	/**
	 * Called if the test reached a point that was not expected.
	 *
	 * @param string the error message
	 * @throws AssertionError always throws an AssertionError
	 */
	protected void fail(String string) {
		println(string);
		throw new AssertionError(string);
	}

	/**
	 * Log an error message.
	 *
	 * @param s the message
	 * @param e the exception
	 */
	public static void logError(String s, Throwable e) {
		if (e == null) {
			e = new Exception(s);
		}
		System.out.println("ERROR: " + s + " " + e.toString() + " ------------------------------");
		System.out.flush();
		e.printStackTrace();
		try {
			TraceSystem ts = new TraceSystem(null, false);
			FileLock lock = new FileLock(ts, "error.lock", 1000);
			lock.lock(FileLock.LOCK_FILE);
			FileWriter fw = new FileWriter("ERROR.txt", true);
			PrintWriter pw = new PrintWriter(fw);
			e.printStackTrace(pw);
			pw.close();
			fw.close();
			lock.unlock();
		} catch (Throwable t) {
			t.printStackTrace();
		}
		System.err.flush();
	}

	/**
	 * Print a message to system out.
	 *
	 * @param s the message
	 */
	protected void println(String s) {
		long time = System.currentTimeMillis() - start;
		printlnWithTime(time, getClass().getName() + " " + s);
	}

	/**
	 * Print a message, prepended with the specified time in milliseconds.
	 *
	 * @param millis the time in milliseconds
	 * @param s the message
	 */
	static void printlnWithTime(long millis, String s) {
		System.out.println(formatTime(millis) + " " + s);
	}

	/**
	 * Print the current time and a message to system out.
	 *
	 * @param s the message
	 */
	protected void printTime(String s) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		println(dateFormat.format(new java.util.Date()) + " " + s);
	}

	/**
	 * Format the time in the format hh:mm:ss.1234 where 1234 is milliseconds.
	 *
	 * @param millis the time in milliseconds
	 * @return the formatted time
	 */
	static String formatTime(long millis) {
		String s = new java.sql.Time(java.sql.Time.valueOf("0:0:0").getTime() + millis).toString()
		+ "." + ("" + (1000 + (millis % 1000))).substring(1);
		if (s.startsWith("00:")) {
			s = s.substring(3);
		}
		return s;
	}

	/**
	 * Delete all database files for this database.
	 *
	 * @param name the database name
	 */
	protected void deleteDb(String name) throws SQLException {
		DeleteDbFiles.execute(baseDir, name, true);
	}

	/**
	 * Delete all database files for a database.
	 *
	 * @param dir the directory where the database files are located
	 * @param name the database name
	 */
	protected void deleteDb(String dir, String name) throws SQLException {
		DeleteDbFiles.execute(dir, name, true);
	}

	/**


    /**
	 * Check if a result set contains the expected data.
	 * The sort order is significant
	 *
	 * @param rs the result set
	 * @param data the expected data
	 * @throws AssertionError if there is a mismatch
	 */
	protected void assertResultSetOrdered(ResultSet rs, String[][] data) throws SQLException {
		assertResultSet(true, rs, data);
	}

	/**
	 * Check if a result set contains the expected data.
	 * The sort order is not significant
	 *
	 * @param rs the result set
	 * @param data the expected data
	 * @throws AssertionError if there is a mismatch
	 */
	//    void assertResultSetUnordered(ResultSet rs, String[][] data) {
	//        assertResultSet(false, rs, data);
	//    }

	/**
	 * Check if a result set contains the expected data.
	 *
	 * @param ordered if the sort order is significant
	 * @param rs the result set
	 * @param data the expected data
	 * @throws AssertionError if there is a mismatch
	 */
	private void assertResultSet(boolean ordered, ResultSet rs, String[][] data) throws SQLException {
		int len = rs.getMetaData().getColumnCount();
		int rows = data.length;
		if (rows == 0) {
			// special case: no rows
			if (rs.next()) {
				fail("testResultSet expected rowCount:" + rows + " got:0");
			}
		}
		int len2 = data[0].length;
		if (len < len2) {
			fail("testResultSet expected columnCount:" + len2 + " got:" + len);
		}
		for (int i = 0; i < rows; i++) {
			if (!rs.next()) {
				fail("testResultSet expected rowCount:" + rows + " got:" + i);
			}
			String[] row = getData(rs, len);
			if (ordered) {
				String[] good = data[i];
				if (!testRow(good, row, good.length)) {
					fail("testResultSet row not equal, got:\n" + formatRow(row) + "\n" + formatRow(good));
				}
			} else {
				boolean found = false;
				for (int j = 0; j < rows; j++) {
					String[] good = data[i];
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
			String[] row = getData(rs, len);
			fail("testResultSet expected rowcount:" + rows + " got:>=" + (rows + 1) + " data:" + formatRow(row));
		}
	}

	private boolean testRow(String[] a, String[] b, int len) {
		for (int i = 0; i < len; i++) {
			String sa = a[i];
			String sb = b[i];
			if (sa == null || sb == null) {
				if (sa != sb) {
					return false;
				}
			} else {
				if (!sa.equals(sb)) {
					return false;
				}
			}
		}
		return true;
	}

	private String[] getData(ResultSet rs, int len) throws SQLException {
		String[] data = new String[len];
		for (int i = 0; i < len; i++) {
			data[i] = rs.getString(i + 1);
			// just check if it works
			rs.getObject(i + 1);
		}
		return data;
	}

	private String formatRow(String[] row) {
		String sb = "";
		for (int i = 0; i < row.length; i++) {
			sb += "{" + row[i] + "}";
		}
		return "{" + sb + "}";
	}

	/**
	 * Simulate a database crash. This method will also close the database
	 * files, but the files are in a state as the power was switched off. It
	 * doesn't throw an exception.
	 *
	 * @param conn the database connection
	 */
	protected void crash(Connection conn) throws SQLException {
		((JdbcConnection) conn).setPowerOffCount(1);
		try {
			conn.createStatement().execute("SET WRITE_DELAY 0");
			conn.createStatement().execute("CREATE TABLE TEST_A(ID INT)");
			fail("should be crashed already");
		} catch (SQLException e) {
			// expected
		}
		try {
			conn.close();
		} catch (SQLException e) {
			// ignore
		}
	}


	/**
	 * Check that a given exception is not an unexpected 'general error'
	 * exception.
	 *
	 * @param e the error
	 */
	protected void assertKnownException(SQLException e) {
		assertKnownException("", e);
	}

	/**
	 * Check that a given exception is not an unexpected 'general error'
	 * exception.
	 *
	 * @param message the message
	 * @param e the exception
	 */
	protected void assertKnownException(String message, SQLException e) {
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
		String className = new Exception().getStackTrace()[1].getClassName();
		org.h2.Driver.load();
		try {
			return (H2TestBase) Class.forName(className).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Can not create object " + className, e);
		}
	}

}
