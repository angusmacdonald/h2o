package org.h2o.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.util.NetUtils;
import org.h2o.H2O;
import org.h2o.H2OLocator;
import org.junit.After;
import org.junit.Before;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

public class H2OTestBase {

    protected static final String DATABASE_NAME = "end_to_end";
    protected static final String DATABASE_BASE_DIRECTORY_PATH = "db_data";
    protected static final int TCP_PORT = 9999;
    protected static final int LOCATOR_PORT = 5999;
    protected static final String USER_NAME = "sa";
    protected static final String PASSWORD = "";

    private final PersistentStateManager persistent_state_manager = new PersistentStateManager(DATABASE_BASE_DIRECTORY_PATH);

    protected H2OLocator locator;
    protected H2O db;

    /**
     * Sets up the test, removing persistent state to be safe.
     * 
     * @throws SQLException if fixture setup fails
     * @throws IOException if fixture setup fails
     */
    @Before
    public void setUp() throws SQLException, IOException {

        Diagnostic.setLevel(DiagnosticLevel.NONE);

        persistent_state_manager.deletePersistentState();
        startup();
    }

    /**
     * Tears down the test, removing persistent state.
     * 
     * @throws SQLException if fixture tear-down fails
     */
    @After
    public void tearDown() throws SQLException {

        shutdown();

        persistent_state_manager.deletePersistentState();
        persistent_state_manager.assertPersistentStateIsAbsent();
    }

    void startup() throws SQLException, IOException {

        //        final String configuration_directory_path = LocalH2OProperties.getConfigurationDirectoryPath(DATABASE_BASE_DIRECTORY_PATH, DATABASE_NAME, String.valueOf(TCP_PORT));
        locator = new H2OLocator(DATABASE_NAME, DATABASE_BASE_DIRECTORY_PATH, LOCATOR_PORT, TCP_PORT, true);
        final String descriptor_file_path = locator.start();

        db = new H2O(DATABASE_NAME, TCP_PORT, DATABASE_BASE_DIRECTORY_PATH, descriptor_file_path);

        db.startDatabase();
    }

    void shutdown() throws SQLException {

        db.shutdown();
        locator.shutdown();
    }

    protected Connection makeConnection() throws SQLException {

        final String jdbcURL = "jdbc:h2:tcp://" + NetUtils.getLocalAddress() + ":" + TCP_PORT + "/" + DATABASE_BASE_DIRECTORY_PATH + "/" + DATABASE_NAME + TCP_PORT;

        // Create connection to the H2O database instance.
        return DriverManager.getConnection(jdbcURL, USER_NAME, PASSWORD);
    }
}
