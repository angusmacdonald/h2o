package org.h2o.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.h2.tools.Server;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.locator.server.LocatorServer;
import org.h2o.test.fixture.TestBase;
import org.h2o.test.fixture.TestQuery;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests of the databases handling of prepared statements.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class PreparedStatementTests extends TestBase {

    /**
     * Tests that prepared statements work in the system where no replication is involved.
     * @throws SQLException 
     */
    @Test
    public void testPreparedStatementsNoReplication() throws SQLException {

        PreparedStatement mStmt = null;
        try {
            mStmt = ca.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 100; i++) {
                mStmt.setInt(1, i);
                mStmt.setString(2, "helloNumber" + i);
                mStmt.addBatch();
            }

            mStmt.executeBatch();

            final int[] pKey = new int[100];
            final String[] secondCol = new String[100];

            pKey[0] = 1;
            pKey[1] = 2;
            secondCol[0] = "Hello";
            secondCol[1] = "World";

            final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST", pKey, secondCol, 3);

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

            validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
        }
        finally {
            if (mStmt != null) {
                mStmt.close();
            }
        }
    }

    /**
     * Tests that prepared statements work in the system where no replication is involved.
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementsUpdateNoReplication() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        PreparedStatement mStmt1 = null;
        PreparedStatement mStmt2 = null;
        try {
            mStmt1 = ca.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 10; i++) {
                mStmt1.setInt(1, i);
                mStmt1.setString(2, "helloNumber" + i);
                mStmt1.addBatch();
            }

            mStmt1.executeBatch();

            mStmt2 = ca.prepareStatement("update PUBLIC.TEST set name=? where id=?;");
            mStmt2.setString(1, "New Order");
            mStmt2.setInt(2, 1);
            mStmt2.addBatch();
            mStmt2.executeBatch();

            final int[] pKey = new int[10];
            final String[] secondCol = new String[10];

            pKey[0] = 1;
            pKey[1] = 2;
            secondCol[0] = "New Order";
            secondCol[1] = "World";

            final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST", pKey, secondCol, 3);

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

            validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
        }
        finally {
            mStmt1.close();
            mStmt2.close();
        }
    }

    /**
     * Tests that prepared statements work in the system where there are two replicas involved.
     * 
     * Tests that prepared statements can correctly be passed to remote machines correctly when the have an AND condition.
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementsUpdate() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        PreparedStatement mStmt1 = null;
        PreparedStatement mStmt2 = null;
        try {
            mStmt1 = ca.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 10; i++) {
                mStmt1.setInt(1, i);
                mStmt1.setString(2, "helloNumber" + i);
                mStmt1.addBatch();
            }

            mStmt1.executeBatch();

            mStmt2 = ca.prepareStatement("update PUBLIC.TEST set name=? where id=? and name=?;");
            mStmt2.setString(1, "New Order");
            mStmt2.setInt(2, 3);
            mStmt2.setString(3, "helloNumber3");
            final int result = mStmt2.executeUpdate();

            assertEquals(1, result); //one replica should have been updated.

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST WHERE ID=3");
            final ResultSet rs = sa.getResultSet();

            if (rs.next()) {
                assertEquals("New Order", rs.getString(2));

                if (rs.next()) {
                    fail("Should only be one entry.");
                }
            }
            else {
                fail("Should be a single entry");
            }
        }
        finally {
            mStmt1.close();
            mStmt2.close();
        }
    }

    /**
     * Tests that prepared statements work in the system where no replication is involved.
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementsDeleteNoReplication() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        PreparedStatement mStmt1 = null;
        PreparedStatement mStmt2 = null;
        try {
            mStmt1 = ca.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 10; i++) {
                mStmt1.setInt(1, i);
                mStmt1.setString(2, "helloNumber" + i);
                mStmt1.addBatch();
            }

            mStmt1.executeBatch();

            mStmt2 = ca.prepareStatement("delete from PUBLIC.TEST where id=?;");
            mStmt2.setInt(1, 9);
            mStmt2.addBatch();
            mStmt2.executeBatch();

            final int[] pKey = new int[9];
            final String[] secondCol = new String[9];

            pKey[0] = 1;
            pKey[1] = 2;
            secondCol[0] = "Hello";
            secondCol[1] = "World";

            final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST", pKey, secondCol, 3);

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

            validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
        }
        finally {
            mStmt1.close();
            mStmt2.close();
        }
    }

    /**
     * Tests that prepared statements work in the system where there are two replicas involved.
     * 
     * Tests that prepared statements can correctly be passed to remote machines correctly when the have an AND condition (for DELETES).
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementsDelete() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        PreparedStatement mStmt1 = null;
        PreparedStatement mStmt2 = null;
        try {
            mStmt1 = ca.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 10; i++) {
                mStmt1.setInt(1, i);
                mStmt1.setString(2, "helloNumber" + i);
                mStmt1.addBatch();
            }

            mStmt1.executeBatch();

            mStmt2 = ca.prepareStatement("delete from PUBLIC.TEST where id=? and name=?;");

            mStmt2.setInt(1, 3);
            mStmt2.setString(2, "helloNumber3");
            mStmt2.addBatch();
            mStmt2.executeBatch();

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST WHERE ID=3");
            final ResultSet rs = sa.getResultSet();

            if (rs.next()) {

                fail("Should only no entries.");

            }
        }
        finally {
            mStmt1.close();
            mStmt2.close();
        }
    }

    @Test
    public void testPreparedStatementsDeleteMultipleConditions() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        sa.executeUpdate("CREATE TABLE TEST2(attr_a INT, attr_b INT, attr_c INT);");
        sb.executeUpdate("CREATE REPLICA TEST2");

        PreparedStatement mStmt1 = null;
        final PreparedStatement mStmt2 = null;
        try {
            mStmt1 = ca.prepareStatement("insert into PUBLIC.TEST2 (attr_a, attr_b, attr_c) values (?,?, ?)");

            for (int i = 1; i < 10; i++) {
                mStmt1.setInt(1, i);
                mStmt1.setInt(2, i);
                mStmt1.setInt(3, 3000 + i);
                mStmt1.addBatch();
            }

            mStmt1.executeBatch();

            mStmt1 = ca.prepareStatement("DELETE FROM test2" + " WHERE attr_a = ?" + " AND attr_b = ?" + " AND attr_c = ?");

            mStmt1.setInt(1, 1);
            mStmt1.setInt(2, 1);
            mStmt1.setInt(3, 3001);
            final int result = mStmt1.executeUpdate();

            assertEquals(1, result);

        }
        finally {
            mStmt1.close();
        }
    }

    /**
    * Tests that prepared statements work in the system where no replication is involved.
     * @throws SQLException 
    */
    @Test
    @Ignore
    public void testPreparedStatementsMultipleTransactions() throws SQLException {

        PreparedStatement mStmt1 = null;
        PreparedStatement mStmt2 = null;

        try {
            final Connection cc = DriverManager.getConnection("jdbc:h2:sm:mem:one", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            cc.setAutoCommit(false);
            mStmt1 = cc.prepareStatement("CREATE TABLE PUBLIC.TEST5 (ID INT, NAME VARCHAR(255));");

            mStmt1.execute();

            cc.commit();

            cc.close();

            while (!cc.isClosed()) {
            };

            mStmt2 = ca.prepareStatement("insert into PUBLIC.TEST5 (id,name) values (?,?)");

            for (int i = 3; i < 100; i++) {
                mStmt2.setInt(1, i);
                mStmt2.setString(2, "helloNumber" + i);
                mStmt2.addBatch();
            }

            mStmt2.executeBatch();

            final int[] pKey = new int[100];
            final String[] secondCol = new String[100];

            pKey[0] = 1;
            pKey[1] = 2;
            secondCol[0] = "Hello";
            secondCol[1] = "World";

            final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST5", pKey, secondCol, 3);

            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST5 ORDER BY ID;");

            validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
        }
        finally {
            if (mStmt1 != null) {
                mStmt1.close();
            }
            if (mStmt2 != null) {
                mStmt2.close();
            }
        }
    }

    /**
     * Tests that prepared statements work in the system where replication is involved.
     * @throws SQLException 
     */
    @Test
    public void testPreparedStatementsReplication() throws SQLException {

        PreparedStatement mStmt = null;
        try {
            createReplicaOnB();

            mStmt = cb.prepareStatement("insert into PUBLIC.TEST (id,name) values (?,?)");

            for (int i = 3; i < 100; i++) {
                mStmt.setInt(1, i);
                mStmt.setString(2, "helloNumber" + i);
                mStmt.addBatch();
            }

            mStmt.executeBatch();

            final int[] pKey = new int[100];
            final String[] secondCol = new String[100];

            pKey[0] = 1;
            pKey[1] = 2;
            secondCol[0] = "Hello";
            secondCol[1] = "World";

            final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST", pKey, secondCol, 3);

            /*
             * If the query hangs and fails at this point its probably because the lock isn't being relinquished when executeBatch is
             * called.
             */
            sa.execute("SELECT LOCAL * FROM PUBLIC.TEST ORDER BY ID;");

            validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
        }
        finally {
            if (mStmt != null) {
                mStmt.close();
            }
        }
    }

    /**
     * Tests that prepared statements work in the system where no replication is involved, and the connection is made through a TCP server.
     * This test uses some of the *Remote classes in H2 which have slightly different behaviour.
     * @throws SQLException 
     * @throws IOException 
     */
    @Test
    public void testPreparedStatementsTcpServer() throws SQLException, IOException {

        /*
         * Reset the locator file. This test doesn't use the in-memory database.
         */
        ls.setRunning(false);
        while (!ls.isFinished()) {
        };

        ls = new LocatorServer(29999, "junitLocator");
        ls.createNewLocatorFile();
        ls.start();

        try {
            DeleteDbFiles.execute("db_data/unittests/", "schema_test", true);
        }
        catch (final SQLException e) {
        }

        TestBase.setUpDescriptorFiles();

        Connection conn = null;
        // start the server, allows to access the database remotely
        Server server = null;
        Statement sa = null;
        PreparedStatement mStmt = null;

        try {
            try {
                server = Server.createTcpServer(new String[]{"-tcpPort", "9990", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test"});
                server.start();

                conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

                sa = conn.createStatement();

                sa.execute("DROP ALL OBJECTS;");
                sa.execute("CREATE TABLE TEST5(ID INT PRIMARY KEY, NAME VARCHAR(255));");
                sa.execute("INSERT INTO TEST5 VALUES(1, 'Hello');");
                sa.execute("INSERT INTO TEST5 VALUES(2, 'World');");
            }
            finally {
                if (conn != null) {
                    conn.close();
                }
                if (sa != null) {
                    sa.close();
                }

                server.shutdown();
                server.stop();
            }

            TestBase.resetLocatorFile();

            try {
                server = Server.createTcpServer(new String[]{"-tcpPort", "9990", "-SMLocation", "jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test"});

                server.start();

                conn = DriverManager.getConnection("jdbc:h2:sm:tcp://localhost:9990/db_data/unittests/schema_test", PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

                mStmt = conn.prepareStatement("insert into PUBLIC.TEST5 (id,name) values (?,?)");

                for (int i = 3; i < 100; i++) {
                    mStmt.setInt(1, i);
                    mStmt.setString(2, "helloNumber" + i);
                    mStmt.addBatch();
                }

                mStmt.executeBatch();

                final int[] pKey = new int[100];
                final String[] secondCol = new String[100];

                pKey[0] = 1;
                pKey[1] = 2;
                secondCol[0] = "Hello";
                secondCol[1] = "World";

                final TestQuery test2query = TestBase.createMultipleInsertStatements("TEST5", pKey, secondCol, 3);

                sa = conn.createStatement();

                sa.execute("SELECT LOCAL * FROM PUBLIC.TEST5 ORDER BY ID;");

                validateResults(test2query.getPrimaryKey(), test2query.getSecondColumn(), sa.getResultSet());
            }
            finally {
                server.shutdown();
                server.stop();
            }
        }
        finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                if (mStmt != null) {
                    mStmt.close();
                }
                if (sa != null) {
                    sa.close();
                }
            }
            catch (final Exception e) {
                //Doesn't matter.
            }

            try {
                DeleteDbFiles.execute("db_data/unittests/", "schema_test", true);
            }
            catch (final SQLException e) {
            }
        }
    }

}
