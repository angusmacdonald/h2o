/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2o.test.h2;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2o.test.fixture.DiskConnectionDriverFactory;
import org.h2o.test.fixture.DiskTestManager;
import org.h2o.test.fixture.H2OTestBase;
import org.h2o.test.fixture.IDiskConnectionDriverFactory;
import org.h2o.test.fixture.ITestManager;
import org.junit.Before;
import org.junit.Test;

import uk.ac.standrews.cs.nds.util.Diagnostic;

/**
 * Test for big result sets.
 */
public class H2TestBigResult extends H2OTestBase {

    private Connection connection;

    private final IDiskConnectionDriverFactory connection_driver_factory = new DiskConnectionDriverFactory();

    private final ITestManager test_manager = new DiskTestManager(1, connection_driver_factory);

    @Override
    public ITestManager getTestManager() {

        return test_manager;
    }

    @Override
    @Before
    public void setUp() throws Exception {

        super.setUp();

        connection = makeConnectionDriver().getConnection();
    }

    @Test
    public void testLargeSubquery() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;
        try {
            stat = connection.createStatement();

            final int len = 4000;
            stat.execute("SET MAX_MEMORY_ROWS " + len / 10);
            stat.execute("CREATE TABLE RECOVERY(TRANSACTION_ID INT, SQL_STMT VARCHAR)");
            stat.execute("INSERT INTO RECOVERY " + "SELECT X, CASE MOD(X, 2) WHEN 0 THEN 'commit' ELSE 'begin' END " + "FROM SYSTEM_RANGE(1, " + len + ")");
            final ResultSet rs = stat.executeQuery("SELECT * FROM RECOVERY WHERE SQL_STMT LIKE 'begin%' AND " + "TRANSACTION_ID NOT IN(SELECT TRANSACTION_ID FROM RECOVERY " + "WHERE SQL_STMT='commit' OR SQL_STMT='rollback')");
            int count = 0, last = 1;
            while (rs.next()) {
                assertEquals(last, rs.getInt(1));
                last += 2;
                count++;
            }
            assertEquals(len / 2, count);
        }
        finally {
            stat.close();
        }
    }

    @Test
    public void testLargeUpdateDelete() throws SQLException {

        Diagnostic.trace();

        final Statement stat = connection.createStatement();

        try {
            final int len = 100000;
            stat.execute("DROP ALL OBJECTS;");
            stat.execute("SET MAX_OPERATION_MEMORY 4096");
            stat.execute("CREATE TABLE TEST AS SELECT * FROM SYSTEM_RANGE(1, " + len + ")");
            stat.execute("UPDATE TEST SET X=X+1");
            stat.execute("DELETE FROM TEST");
        }
        finally {
            stat.close();
        }
    }

    @Test
    public void testLimitBufferedResult() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;

        try {
            stat = connection.createStatement();

            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST(ID INT)");
            for (int i = 0; i < 200; i++) {
                stat.execute("INSERT INTO TEST(ID) VALUES(" + i + ")");
            }
            stat.execute("SET MAX_MEMORY_ROWS 100");
            ResultSet rs;
            rs = stat.executeQuery("select id from test order by id limit 10 offset 85");
            for (int i = 85; rs.next(); i++) {
                assertEquals(i, rs.getInt(1));
            }
            rs = stat.executeQuery("select id from test order by id limit 10 offset 95");
            for (int i = 95; rs.next(); i++) {
                assertEquals(i, rs.getInt(1));
            }
            rs = stat.executeQuery("select id from test order by id limit 10 offset 105");
            for (int i = 105; rs.next(); i++) {
                assertEquals(i, rs.getInt(1));
            }
        }
        finally {
            stat.close();
        }
    }

    @Test
    public void testOrderGroup() throws SQLException {

        Diagnostic.trace();

        Statement statement1 = null;
        Statement statement2 = null;
        PreparedStatement prep1 = null;
        PreparedStatement prep2 = null;

        try {
            statement1 = connection.createStatement();
            statement1.execute("DROP TABLE IF EXISTS TEST");
            statement1.execute("CREATE TABLE TEST(" + "ID INT PRIMARY KEY, " + "Name VARCHAR(255), " + "FirstName VARCHAR(255), " + "Points INT," + "LicenseID INT)");
            final int len = 5000;
            prep1 = connection.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?, ?, ?)");
            for (int i = 0; i < len; i++) {
                prep1.setInt(1, i);
                prep1.setString(2, "Name " + i);
                prep1.setString(3, "First Name " + i);
                prep1.setInt(4, i * 10);
                prep1.setInt(5, i * i);
                prep1.execute();
            }
            statement1.close();

            statement1 = connection.createStatement();
            statement1.setMaxRows(len + 1);
            ResultSet rs = statement1.executeQuery("SELECT * FROM TEST ORDER BY ID");
            for (int i = 0; i < len; i++) {
                rs.next();
                assertEquals(i, rs.getInt(1));
                assertEquals("Name " + i, rs.getString(2));
                assertEquals("First Name " + i, rs.getString(3));
                assertEquals(i * 10, rs.getInt(4));
                assertEquals(i * i, rs.getInt(5));
            }

            statement1.setMaxRows(len + 1);
            rs = statement1.executeQuery("SELECT * FROM TEST WHERE ID >= 1000 ORDER BY ID");
            for (int i = 1000; i < len; i++) {
                rs.next();
                assertEquals(i, rs.getInt(1));
                assertEquals("Name " + i, rs.getString(2));
                assertEquals("First Name " + i, rs.getString(3));
                assertEquals(i * 10, rs.getInt(4));
                assertEquals(i * i, rs.getInt(5));
            }

            statement1.execute("SET MAX_MEMORY_ROWS 2");
            rs = statement1.executeQuery("SELECT Name, SUM(ID) FROM TEST GROUP BY NAME");
            while (rs.next()) {
                rs.getString(1);
                rs.getInt(2);
            }

            connection.setAutoCommit(false);
            statement1.setMaxRows(0);
            statement1.execute("SET MAX_MEMORY_ROWS 0");
            statement1.execute("CREATE TABLE DATA(ID INT, NAME VARCHAR_IGNORECASE(255))");
            prep2 = connection.prepareStatement("INSERT INTO DATA VALUES(?, ?)");
            for (int i = 0; i < len; i++) {
                prep2.setInt(1, i);
                prep2.setString(2, "" + i / 200);
                prep2.execute();
            }
            statement2 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            rs = statement2.executeQuery("SELECT NAME FROM DATA");
            rs.last();
            connection.commit();
            connection.setAutoCommit(true);

            rs = statement2.executeQuery("SELECT NAME FROM DATA ORDER BY ID");
            while (rs.next()) {
                // do nothing
            }
        }
        finally {
            closeIfNotNull(statement1);
            closeIfNotNull(statement2);
            closeIfNotNull(prep1);
            closeIfNotNull(prep2);
        }
    }

    @Test
    public void testDeleteAllObjects() throws SQLException {

        Diagnostic.trace();

        Statement stat = null;
        try {
            stat = connection.createStatement();
            final int len = 100000;
            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST(ID INT)");
            stat.execute("DROP ALL OBJECTS;");
            stat.execute("DROP TABLE IF EXISTS TEST");
            stat.execute("CREATE TABLE TEST AS SELECT * FROM SYSTEM_RANGE(1, " + len + ")");
        }
        finally {
            stat.close();
        }
    }
}
