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

import org.h2.test.TestBase;

/**
 * Temporary table tests.
 */
public class TestTempTables extends TestBase {

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

        deleteDb("tempTables");
        final Connection c1 = getConnection("tempTables");
        final Connection c2 = getConnection("tempTables");
        testConstraints(c1, c2);
        testTables(c1, c2);
        testIndexes(c1, c2);
        c1.close();
        c2.close();
        deleteDb("tempTables");
    }

    private void testConstraints(final Connection conn1, final Connection conn2) throws SQLException {

        final Statement s1 = conn1.createStatement(), s2 = conn2.createStatement();
        s1.execute("create local temporary table test(id int unique)");
        s2.execute("create local temporary table test(id int unique)");
        s1.execute("alter table test add constraint a unique(id)");
        s2.execute("alter table test add constraint a unique(id)");
        s1.execute("drop table test");
        s2.execute("drop table test");
    }

    private void testIndexes(final Connection conn1, final Connection conn2) throws SQLException {

        conn1.createStatement().executeUpdate("create local temporary table test(id int)");
        conn1.createStatement().executeUpdate("create index idx_id on test(id)");
        conn2.createStatement().executeUpdate("create local temporary table test(id int)");
        conn2.createStatement().executeUpdate("create index idx_id on test(id)");
        conn2.createStatement().executeUpdate("drop index idx_id");
        conn2.createStatement().executeUpdate("drop table test");
        conn2.createStatement().executeUpdate("create table test(id int)");
        conn2.createStatement().executeUpdate("create index idx_id on test(id)");
        conn1.createStatement().executeUpdate("drop table test");
        conn1.createStatement().executeUpdate("drop table test");
    }

    private void testTables(final Connection c1, final Connection c2) throws SQLException {

        final Statement s1 = c1.createStatement();
        final Statement s2 = c2.createStatement();
        s1.execute("CREATE LOCAL TEMPORARY TABLE LT(A INT)");
        s1.execute("CREATE GLOBAL TEMPORARY TABLE GT1(ID INT)");
        s2.execute("CREATE GLOBAL TEMPORARY TABLE GT2(ID INT)");
        s2.execute("CREATE LOCAL TEMPORARY TABLE LT(B INT)");
        s2.execute("SELECT B FROM LT");
        s1.execute("SELECT A FROM LT");
        s1.execute("SELECT * FROM GT1");
        s2.execute("SELECT * FROM GT1");
        s1.execute("SELECT * FROM GT2");
        s2.execute("SELECT * FROM GT2");
        s2.execute("DROP TABLE GT1");
        s2.execute("DROP TABLE GT2");
        s2.execute("DROP TABLE LT");
        s1.execute("DROP TABLE LT");

        // temp tables: 'on commit' syntax is currently not documented, because
        // not tested well
        // and hopefully nobody is using it, as it looks like functional sugar
        // (this features are here for compatibility only)
        ResultSet rs;
        c1.setAutoCommit(false);
        s1.execute("create local temporary table test_temp(id int) on commit delete rows");
        s1.execute("insert into test_temp values(1)");
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(rs, 1);
        c1.commit();
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(rs, 0);
        s1.execute("drop table test_temp");

        s1.execute("create local temporary table test_temp(id int) on commit drop");
        s1.execute("insert into test_temp values(1)");
        rs = s1.executeQuery("select * from test_temp");
        assertResultRowCount(rs, 1);
        c1.commit();
        try {
            rs = s1.executeQuery("select * from test_temp");
            fail("test_temp should have been dropped automatically");
        }
        catch (final SQLException e) {
            assertKnownException(e);
        }
    }

}
