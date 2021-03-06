/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import org.h2.constant.ErrorCode;
import org.h2.test.TestBase;

/**
 * Tests out of memory situations. The database must not get corrupted, and transactions must stay atomic.
 */
public class TestOutOfMemory extends TestBase {

    private LinkedList list = new LinkedList();

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

        if (config.memory || config.mvcc) { return; }
        for (int i = 0; i < 5; i++) {
            System.gc();
        }
        deleteDb("outOfMemory");
        final Connection conn = getConnection("outOfMemory");
        final Statement stat = conn.createStatement();
        stat.execute("drop all objects");
        stat.execute("create table stuff (id int, text varchar as space(100) || id)");
        stat.execute("insert into stuff(id) select x from system_range(1, 2000)");
        final PreparedStatement prep = conn.prepareStatement("update stuff set text = text || ' upd'");
        prep.execute();
        eatMemory(80);
        try {
            try {
                prep.execute();
                fail();
            }
            catch (final SQLException e) {
                assertEquals(ErrorCode.OUT_OF_MEMORY, e.getErrorCode());
            }
            list = null;
            final ResultSet rs = stat.executeQuery("select count(*) from stuff");
            rs.next();
            assertEquals(2000, rs.getInt(1));
        }
        finally {
            conn.close();
        }
        deleteDb("outOfMemory");
    }

    private void eatMemory(final int remainingKB) {

        byte[] reserve = new byte[remainingKB * 1024];
        int max = 128 * 1024 * 1024;
        int div = 2;
        while (true) {
            final long free = Runtime.getRuntime().freeMemory();
            final long freeTry = free / div;
            final int eat = (int) Math.min(max, freeTry);
            try {
                final byte[] block = new byte[eat];
                list.add(block);
            }
            catch (final OutOfMemoryError e) {
                if (eat < 32) {
                    break;
                }
                if (eat == max) {
                    max /= 2;
                    if (max < 128) {
                        break;
                    }
                }
                if (eat == freeTry) {
                    div += 1;
                }
                else {
                    div = 2;
                }
            }
        }
        // silly code - makes sure there are no warnings
        reserve[0] = reserve[1];
        // actually it is anyway garbage collected
        reserve = null;
    }

}
