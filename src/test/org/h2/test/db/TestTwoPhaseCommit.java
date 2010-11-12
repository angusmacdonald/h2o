/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2.test.TestBase;

/**
 * Tests for the two-phase-commit feature.
 */
public class TestTwoPhaseCommit extends TestBase {

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

        if (config.memory || config.networked || config.logMode == 0) { return; }

        deleteDb("twoPhaseCommit");

        prepare();
        openWith(true);
        test(true);

        prepare();
        openWith(false);
        test(false);
        deleteDb("twoPhaseCommit");
    }

    private void test(final boolean rolledBack) throws SQLException, IOException {

        final Connection conn = getConnection("twoPhaseCommit");
        final Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        final ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(rs.getInt(1), 1);
        assertEquals(rs.getString(2), "Hello");
        if (!rolledBack) {
            rs.next();
            assertEquals(rs.getInt(1), 2);
            assertEquals(rs.getString(2), "World");
        }
        assertFalse(rs.next());
        conn.close();
    }

    private void openWith(final boolean rollback) throws SQLException, IOException {

        final Connection conn = getConnection("twoPhaseCommit");
        final Statement stat = conn.createStatement();
        final ArrayList list = new ArrayList();
        final ResultSet rs = stat.executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT");
        while (rs.next()) {
            list.add(rs.getString("TRANSACTION"));
        }
        for (int i = 0; i < list.size(); i++) {
            final String s = (String) list.get(i);
            if (rollback) {
                stat.execute("ROLLBACK TRANSACTION " + s);
            }
            else {
                stat.execute("COMMIT TRANSACTION " + s);
            }
        }
        conn.close();
    }

    private void prepare() throws SQLException, IOException {

        deleteDb("twoPhaseCommit");
        final Connection conn = getConnection("twoPhaseCommit");
        final Statement stat = conn.createStatement();
        stat.execute("SET WRITE_DELAY 0");
        conn.setAutoCommit(false);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        conn.commit();
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        stat.execute("PREPARE COMMIT XID_TEST_TRANSACTION_WITH_LONG_NAME");
        crash(conn);
    }
}
