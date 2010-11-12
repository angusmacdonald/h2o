/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.DatabaseEventListener;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * Tests the DatabaseEventListener.
 */
public class TestListener extends TestBase implements DatabaseEventListener {

    private long last;

    private int lastState = -1;

    private String url;

    public TestListener() {

        start = last = System.currentTimeMillis();
    }

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

        if (config.networked || config.cipher != null) { return; }
        deleteDb("listener");
        Connection conn;
        conn = getConnection("listener");
        final Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        final PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Test' || SPACE(100))");
        final int len = getSize(100, 100000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.execute();
        }
        crash(conn);

        conn = getConnection("listener;database_event_listener='" + getClass().getName() + "'");
        conn.close();
        deleteDb("listener");
    }

    @Override
    public void diskSpaceIsLow(final long stillAvailable) {

        printTime("diskSpaceIsLow");
    }

    @Override
    public void exceptionThrown(final SQLException e, final String sql) {

        TestBase.logError("exceptionThrown sql=" + sql, e);
    }

    @Override
    public void setProgress(final int state, String name, final int current, final int max) {

        final long time = System.currentTimeMillis();
        if (state == lastState && time < last + 1000) { return; }
        if (name.length() > 30) {
            name = "..." + name.substring(name.length() - 30);
        }
        last = time;
        lastState = state;
        String stateName;
        switch (state) {
            case STATE_SCAN_FILE:
                stateName = "Scan " + name;
                break;
            case STATE_CREATE_INDEX:
                stateName = "Create Index " + name;
                break;
            case STATE_RECOVER:
                stateName = "Recover";
                break;
            default:
                TestBase.logError("unknown state: " + state, null);
                stateName = "? " + name;
        }
        try {
            Thread.sleep(1);
        }
        catch (final InterruptedException e) {
            // ignore
        }
        printTime("state: " + stateName + " " + 100 * current / max + " " + (time - start));
    }

    @Override
    public void closingDatabase() {

        if (url.toUpperCase().indexOf("CIPHER") >= 0) { return; }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, getUser(), getPassword());
            conn.createStatement().execute("DROP TABLE TEST2");
            conn.close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
        finally {
            JdbcUtils.closeSilently(conn);
        }
    }

    @Override
    public void init(final String url) {

        this.url = url;
    }

    @Override
    public void opened() {

        if (url.toUpperCase().indexOf("CIPHER") >= 0) { return; }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, getUser(), getPassword());
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TEST2(ID INT)");
            conn.close();
        }
        catch (final SQLException e) {
            e.printStackTrace();
        }
        finally {
            JdbcUtils.closeSilently(conn);
        }
    }

}
