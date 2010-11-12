/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;

/**
 * Various small performance tests.
 */
public class TestSpeed extends TestBase {

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

        deleteDb("speed");
        Connection conn;

        conn = getConnection("speed");

        final Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        final int len = getSize(1, 10000);
        for (int i = 0; i < len; i++) {
            stat.execute("SELECT ID, NAME FROM TEST ORDER BY ID");
        }

        long time = System.currentTimeMillis();

        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE CACHED TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");

        final int max = getSize(1, 10000);
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.setString(2, "abchelloasdfaldsjflajdflajdslfoajlskdfkjasdfadsfasdfadsfadfsalksdjflasjflajsdlkfjaksdjflkskd" + i);
            prep.execute();
        }

        time = System.currentTimeMillis() - time;
        trace(time + " insert");

        time = System.currentTimeMillis();

        prep = conn.prepareStatement("UPDATE TEST SET NAME='Another data row which is long' WHERE ID=?");
        for (int i = 0; i < max; i++) {
            prep.setInt(1, i);
            prep.execute();

            time = System.currentTimeMillis() - time;
            trace(time + " update");

            conn.close();
            time = System.currentTimeMillis() - time;
            trace(time + " close");
            deleteDb("speed");
        }
    }
}
