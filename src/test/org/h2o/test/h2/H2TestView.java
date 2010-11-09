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

import org.h2o.test.H2OTestBase;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * Test for views.
 */
public class H2TestView extends H2OTestBase {

    private Connection connection;

    @Override
    protected int getNumberOfDatabases() {

        return 1;
    }

    @Override
    @Before
    public void setUp() throws SQLException, IOException {

        super.setUp();

        connection = getConnections()[0];
    }

    @Test
    public void testInSelect() throws SQLException {

        Diagnostic.trace();

        Statement statement = null;

        try {
            statement = connection.createStatement();
            statement.execute("create table test(id int primary key) as select 1");
            final PreparedStatement prep = connection.prepareStatement("select * from test t where t.id in (select t2.id from test t2 where t2.id in (?, ?))");
            prep.setInt(1, 1);
            prep.setInt(2, 2);
            prep.execute();
        }
        finally {
            closeIfNotNull(statement);
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
