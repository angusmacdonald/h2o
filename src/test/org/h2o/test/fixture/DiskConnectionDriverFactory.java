package org.h2o.test.fixture;

import java.sql.Connection;
import java.util.Set;

import org.h2o.test.endtoend.fixture.EndToEndConnectionDriver;

public class DiskConnectionDriverFactory implements IDiskConnectionDriverFactory {

    @Override
    public ConnectionDriver makeConnectionDriver(final int db_port, final String database_base_directory_path, final String database_name, final String username, final String password, final Set<Connection> connections_to_be_closed) {

        return new EndToEndConnectionDriver(db_port, database_base_directory_path, database_name, username, password, connections_to_be_closed);
    }
}
