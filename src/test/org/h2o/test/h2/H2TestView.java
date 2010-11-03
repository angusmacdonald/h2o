/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for views.
 */
public class H2TestView extends H2TestBase {

    private LocatorServer ls;

    @Before
    public void setUp() throws SQLException {

        DeleteDbFiles.execute("data\\test\\", "view", true);
        DeleteDbFiles.execute("data\\test\\", "view2", true);

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        config = new TestAll();

        if (config.memory) { return; }
    }

    @After
    public void tearDown() throws SQLException {

        ls.setRunning(false);
        while (!ls.isFinished()) {
        };

        DeleteDbFiles.execute("data\\test\\", "view", true);
        DeleteDbFiles.execute("data\\test\\", "view2", true);

    }

    @Test
    public void testInSelect() throws SQLException {

        Connection conn = null;

        Statement stat = null;

        try {
            conn = getConnection("view");

            stat = conn.createStatement();
            stat.execute("create table test(id int primary key) as select 1");
            final PreparedStatement prep = conn.prepareStatement("select * from test t where t.id in (select t2.id from test t2 where t2.id in (?, ?))");
            prep.setInt(1, 1);
            prep.setInt(2, 2);
            prep.execute();
        }
        finally {
            conn.close();
            stat.close();
        }
    }

    @Test
    public void testUnionReconnect() throws SQLException {

        if (config.memory) { return; }
        Connection conn = null;

        Statement stat = null;

        try {
            conn = getConnection("view");

            stat = conn.createStatement();
            stat.execute("create table t1(k smallint, ts timestamp(6))");
            stat.execute("create table t2(k smallint, ts timestamp(6))");
            stat.execute("create table t3(k smallint, ts timestamp(6))");
            stat.execute("create view v_max_ts as select " + "max(ts) from (select max(ts) as ts from t1 " + "union select max(ts) as ts from t2 " + "union select max(ts) as ts from t3)");
            stat.execute("create view v_test as select max(ts) as ts from t1 " + "union select max(ts) as ts from t2 " + "union select max(ts) as ts from t3");
            conn.close();
            conn = getConnection("view");
            stat = conn.createStatement();
            stat.execute("select * from v_max_ts");
        }
        finally {
            conn.close();
            stat.close();
            deleteDb("view");
        }
    }
}
