/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.constant.SysProperties;
import org.h2.store.FileLister;
import org.h2.test.TestBase;

/**
 * Tests the database transaction log file.
 */
public class TestLogFile extends TestBase {

    private static final int MAX_LOG_SIZE = 1;

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

    private long reconnect(final int maxFiles) throws SQLException, IOException {

        if (conn != null) {
            conn.close();
        }
        long length = 0;
        final ArrayList files = FileLister.getDatabaseFiles(baseDir, "logfile", false);
        assertSmaller(files.size(), maxFiles + 2);
        for (int i = 0; i < files.size(); i++) {
            final String fileName = (String) files.get(i);
            final long len = new File(fileName).length();
            length += len;
        }
        conn = getConnection("logfile");
        return length;
    }

    @Override
    public void test() throws SQLException, IOException {

        if (config.memory) { return; }
        deleteDb("logfile");
        final int old = SysProperties.getLogFileDeleteDelay();
        System.setProperty(SysProperties.H2_LOG_DELETE_DELAY, "0");
        try {
            reconnect(0);
            insert();
            // data, index, log
            final int maxFiles = 3;
            for (int i = 0; i < 3; i++) {
                final long length = reconnect(maxFiles);
                insert();
                final long l2 = reconnect(maxFiles);
                trace("length:" + length + " l2:" + l2);
                assertTrue(l2 <= length * 2);
            }
            conn.close();
        }
        finally {
            System.setProperty(SysProperties.H2_LOG_DELETE_DELAY, "" + old);
        }
        deleteDb("logfile");
    }

    private void checkLogSize() throws SQLException {

        final String[] files = new File(".").list();
        for (final String name : files) {
            if (name.startsWith("logfile") && name.endsWith(".log.db")) {
                final long length = new File(name).length();
                assertSmaller(length, MAX_LOG_SIZE * 1024 * 1024 * 2);
            }
        }
    }

    private void insert() throws SQLException {

        final Statement stat = conn.createStatement();
        stat.execute("SET LOGSIZE 200");
        stat.execute("SET MAX_LOG_SIZE " + MAX_LOG_SIZE);
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        final PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, 'Hello' || ?)");
        final int len = getSize(1, 10000);
        for (int i = 0; i < len; i++) {
            prep.setInt(1, i);
            prep.setInt(2, i);
            prep.execute();
            if (i > 0 && i % 2000 == 0) {
                checkLogSize();
            }
        }
        checkLogSize();
        // stat.execute("TRUNCATE TABLE TEST");
    }

}
