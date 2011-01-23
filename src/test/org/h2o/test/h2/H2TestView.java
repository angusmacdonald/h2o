/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.h2o.test.fixture.DiskConnectionDriverFactory;
import org.h2o.test.fixture.DiskTestManager;
import org.h2o.test.fixture.H2OTestBase;
import org.h2o.test.fixture.IDiskConnectionDriverFactory;
import org.h2o.test.fixture.ITestManager;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.madface.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

import com.mindbright.ssh2.SSH2Exception;

/**
 * Test for views.
 */
public class H2TestView extends H2OTestBase {

    private Connection connection;

    private final IDiskConnectionDriverFactory connection_driver_factory = new DiskConnectionDriverFactory();

    private final ITestManager test_manager = new DiskTestManager(1, connection_driver_factory);

    @Override
    public ITestManager getTestManager() {

        return test_manager;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException, SSH2Exception, TimeoutException {

        super.setUp();

        connection = makeConnectionDriver().getConnection();
    }

    @Test
    public void testInSelect() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;
        PreparedStatement prep = null;

        try {
            statement = connection.createStatement();
            statement.execute("create table test(id int primary key) as select 1");
            prep = connection.prepareStatement("select * from test t where t.id in (select t2.id from test t2 where t2.id in (?, ?))");
            prep.setInt(1, 1);
            prep.setInt(2, 2);
            prep.execute();
        }
        finally {
            closeIfNotNull(statement);
            closeIfNotNull(prep);
        }
    }

    @Test
    public void testUnionReconnect() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("create table t1(k smallint, ts timestamp(6))");
            statement.execute("create table t2(k smallint, ts timestamp(6))");
            statement.execute("create table t3(k smallint, ts timestamp(6))");
            statement.execute("create view v_max_ts as select " + "max(ts) from (select max(ts) as ts from t1 " + "union select max(ts) as ts from t2 " + "union select max(ts) as ts from t3)");
            statement.execute("create view v_test as select max(ts) as ts from t1 " + "union select max(ts) as ts from t2 " + "union select max(ts) as ts from t3");

            statement = connection.createStatement();
            statement.execute("select * from v_max_ts");
        }
        finally {
            closeIfNotNull(statement);
        }
    }
}
