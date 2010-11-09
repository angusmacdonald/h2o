package org.h2o.autonomic.settings;

/**
 * Stores various settings used to indicate certain types of unit tests. This is used to trigger certain types of errors in H2O
 * and can be used to simulate the failure of queries.
 * 
 * <p>Don't change these values unless you are creating a unit test.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class TestingSettings {

    /**
     * Is the database running as part of a JUnit test. A test may choose to set this to true to induce failure in update propagation - this
     * is used to test the system's ability to rollback inserts on this type of failure.
     */
    public static boolean IS_TESTING_PRE_COMMIT_FAILURE = false;

    public static boolean IS_TESTING_PRE_PREPARE_FAILURE = false;

    public static boolean IS_TESTING_QUERY_FAILURE = false;

    public static boolean IS_TESTING_CREATETABLE_FAILURE = false;

    /**
     * Used in testing.
     */
    public static String DEFAULT_SCHEMA_MANAGER_LOCATION = "jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test";

    public static boolean IS_TESTING_H2_TESTS = false;

}
