/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.test;

import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.h2o.test.fixture.TestBase;
import org.h2o.test.util.ReadBenchmarkQueriesFromFile;
import org.junit.Test;

import uk.ac.standrews.cs.nds.rpc.AbstractConnectionPool;

/**
 * Tests that check for problems that have previous occurred when running the PolePosition and BenchmarkSQL benchmarking tools.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class BenchmarkTests extends TestBase {

    /**
     * Tests that prepared statements work in the system where no replication is involved.
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementAutoCommitOff() throws SQLException {

        // update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
        createReplicaOnB();

        PreparedStatement mStmt = null;
        Statement stt = null;
        try {
            ca.setAutoCommit(false);

            stt = ca.createStatement();
            stt.execute("drop table if exists australia");
            stt.execute("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");

            ca.commit();

            ca.setAutoCommit(false);

            mStmt = ca.prepareStatement("insert into australia values(?,?,?,?,?)");

            mStmt.setInt(1, 1);
            mStmt.setString(2, "name");
            mStmt.setString(3, "firstName");
            mStmt.setInt(4, 10);
            mStmt.setInt(5, 400);
            mStmt.addBatch();

            ResultSet rs = stt.executeQuery("select * from australia");

            /*
             * H2O used to throw an error here because the prepared statement command would use the 'CALL AUTOCOMMIT()' query to find
             * whether auto commit was on, and this call would somehow result in a commit being made.
             */
            if (rs.next()) {
                fail("The table shouldn't have any entries yet. Update hasn't happened.");
            }

            System.err.println("About to execute batch.");
            mStmt.executeBatch();
            System.err.println("Executed batch.");

            System.err.println("About to execute select.");

            final Statement statement = cb.createStatement();
            statement.executeQuery("select * from australia");
            statement.close();

            // Execute batch seems to auto-commit, so the table's already exist before this point.

            ca.commit();

            ca.setAutoCommit(false);

            rs = stt.executeQuery("select * from australia");

            if (!rs.next()) {
                fail("The table should have one entry.");
            }

            ca.setAutoCommit(false);

            stt.execute("drop table australia");

            stt.execute("create table australia (ID  INTEGER NOT NULL, Name VARCHAR(100), " + "FirstName VARCHAR(100), Points INTEGER, LicenseID INTEGER, PRIMARY KEY(ID))");

            ca.commit();
        }
        finally {
            stt.close();
            mStmt.close();
        }
    }

    /**
     * Runs a set of queries from BenchmarkSQL tool, specified in the file 'testQueries/benchmarkSQL-1.txt'.
     * @throws SQLException
     * @throws IOException Error reading from the benchmark file.
     * @throws FileNotFoundException Failed to read from the benchmark file.
     */
    @Test
    public void benchmarkSQLQueriesBasic() throws SQLException, FileNotFoundException, IOException {

        createBenchmarkSQLTables();

        runBenchmarkCode("testQueries/benchmarkSQL-1.txt");

    }

    /**
     * Runs a set of queries from BenchmarkSQL tool, specified in the file 'testQueries/benchmarkSQL-1.txt'.
     * 
     * Replicas of all tables exist on another machine as well.
     * 
     * @throws SQLException
     * @throws IOException Error reading from the benchmark file.
     * @throws FileNotFoundException Failed to read from the benchmark file.
     */
    @Test
    public void benchmarkSQLQueriesBasicWithReplication() throws SQLException, FileNotFoundException, IOException {

        createBenchmarkSQLTables();

        createReplicaOnB("warehouse");
        createReplicaOnB("district");
        createReplicaOnB("customer");
        createReplicaOnB("history");
        createReplicaOnB("oorder");
        createReplicaOnB("new_order");
        createReplicaOnB("stock");
        createReplicaOnB("item");

        runBenchmarkCode("testQueries/benchmarkSQL-2.txt");

    }

    /**
     * Runs a set of queries from BenchmarkSQL tool, specified in the file 'testQueries/benchmarkSQL-2.txt'.
     * 
     * More SQL statements are executed than with {@link #testBenchmarkSQLQueriesBasic()}
     * @throws SQLException
     * @throws IOException Error reading from the benchmark file.
     * @throws FileNotFoundException Failed to read from the benchmark file.
     */
    @Test
    public void benchmarkSQLQueriesMore() throws SQLException, FileNotFoundException, IOException {

        createBenchmarkSQLTables();

        runBenchmarkCode("testQueries/benchmarkSQL-2.txt");

    }

    /**
     * Executes an entire run-through of benchmarkSQL.
     * @throws SQLException
     * @throws IOException Error reading from the benchmark file.
     * @throws FileNotFoundException Failed to read from the benchmark file.
     */
    @Test
    public void benchmarkSQLQueriesFull() throws SQLException, FileNotFoundException, IOException {

        createBenchmarkSQLTables();

        runBenchmarkCode("testQueries/benchmarkSQL-full.txt");

    }

    /**
     * Executes an entire run-through of benchmarkSQL, with multiple replicas for each table.
     * @throws SQLException
     * @throws IOException Error reading from the benchmark file.
     * @throws FileNotFoundException Failed to read from the benchmark file.
     */
    @Test
    public void benchmarkSQLQueriesFullTwoReplicas() throws SQLException, FileNotFoundException, IOException {

        AbstractConnectionPool.MAX_FREE_CONNECTIONS_PER_ADDRESS = 10;
        createBenchmarkSQLTables();

        createReplicaOnB("warehouse");
        createReplicaOnB("district");
        createReplicaOnB("customer");
        createReplicaOnB("history");
        createReplicaOnB("oorder");
        createReplicaOnB("new_order");
        createReplicaOnB("stock");
        createReplicaOnB("item");

        runBenchmarkCode("testQueries/benchmarkSQL-full.txt");

    }

    /*
     * 
     * UTILITY FUNCTIONS
     * 
     */

    public void runBenchmarkCode(final String fileName) throws FileNotFoundException, IOException, SQLException {

        final ArrayList<String> updateQueries = ReadBenchmarkQueriesFromFile.getSQLQueriesFromFile(fileName);

        executeQueries(updateQueries);
    }

    public void createBenchmarkSQLTables() throws FileNotFoundException, IOException, SQLException {

        final ArrayList<String> createTableQueries = ReadBenchmarkQueriesFromFile.getSQLQueriesFromFile("testQueries/benchmarkSQL-createTables.txt");

        executeQueries(createTableQueries);
    }

    public void executeQueries(final ArrayList<String> queries) throws SQLException {

        Statement stt = null;
        try {
            stt = ca.createStatement();

            for (final String sql : queries) {

                stt.execute(sql);
            }
        }
        finally {
            stt.close();
        }
    }

}
