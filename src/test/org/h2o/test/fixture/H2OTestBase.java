package org.h2o.test.fixture;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;

import uk.ac.standrews.cs.nds.madface.exceptions.UnknownPlatformException;
import uk.ac.standrews.cs.nds.util.UndefinedDiagnosticLevelException;

import com.mindbright.ssh2.SSH2Exception;

public abstract class H2OTestBase {

    public abstract ITestManager getTestManager();

    // -------------------------------------------------------------------------------------------------------

    /**
     * Sets up the test.
     *
     * @throws SQLException if fixture setup fails
     * @throws IOException if fixture setup fails
     * @throws UnknownPlatformException
     * @throws UndefinedDiagnosticLevelException
     * @throws TimeoutException
     * @throws SSH2Exception
     */
    @Before
    public void setUp() throws SQLException, IOException, UnknownPlatformException, UndefinedDiagnosticLevelException, SSH2Exception, TimeoutException {

        getTestManager().setUp();
    }

    /**
     * Tears down the test, removing persistent state.
     *
     * @throws SQLException if fixture tear-down fails
     */
    @After
    public void tearDown() throws SQLException {

        getTestManager().tearDown();
    }

    // -------------------------------------------------------------------------------------------------------

    public void startup() throws IOException, UnknownPlatformException, SSH2Exception, TimeoutException {

        getTestManager().startup();
    }

    public void shutdown() {

        getTestManager().shutdown();
    }

    // -------------------------------------------------------------------------------------------------------

    protected ConnectionDriver makeConnectionDriver() {

        return makeConnectionDriver(0);
    }

    protected ConnectionDriver makeConnectionDriver(final int db_index) {

        return getTestManager().makeConnectionDriver(db_index);
    }

    protected void closeIfNotNull(final Statement statement) {

        try {
            if (statement != null) {
                statement.close();
            }
        }
        catch (final SQLException e) {
            // Ignore and carry on, only trying to tidy up.
        }
    }
}
