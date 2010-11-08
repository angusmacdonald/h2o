/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestAll;
import org.h2.tools.DeleteDbFiles;
import org.h2o.locator.server.LocatorServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * Test for views.
 */
public class H2TestViewFailing extends H2TestBase {

    private LocatorServer ls;

    @Before
    public void setUp() throws SQLException, IOException {

        DeleteDbFiles.execute("data\\test\\", "view", true);
        DeleteDbFiles.execute("data\\test\\", "view2", true);

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        config = new TestAll();

        Diagnostic.setLevel(DiagnosticLevel.FULL);
    }

    @After
    public void tearDown() throws SQLException, InterruptedException {

        ls.setRunning(false);
        while (!ls.isFinished()) {
            Thread.sleep(SHUTDOWN_CHECK_DELAY);
        };

        DeleteDbFiles.execute("data\\test\\", "view", true);
        DeleteDbFiles.execute("data\\test\\", "view2", true);
    }

    @Test(timeout = 60000)
    public void testManyViews() throws SQLException {

        Diagnostic.trace(DiagnosticLevel.FULL);

        Connection conn = null;

        Statement s = null;

        try {
            conn = getConnection("view2");

            s = conn.createStatement();
            s.execute("create table t0(id int primary key)");
            s.execute("insert into t0 values(1), (2), (3)");
            for (int i = 0; i < 30; i++) {
                s.execute("create view t" + (i + 1) + " as select * from t" + i);
                s.execute("select * from t" + (i + 1));
                final ResultSet rs = s.executeQuery("select count(*) from t" + (i + 1) + " where id=2");
                assertTrue(rs.next());
                assertEquals(rs.getInt(1), 1);
            }
        }
        finally {
            s.close();
            conn.close();
            conn = getConnection("view");
            conn.close();
            deleteDb("view");
        }
    }
}
