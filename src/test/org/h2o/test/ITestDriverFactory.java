package org.h2o.test;

import java.sql.Connection;
import java.util.Set;

public interface ITestDriverFactory {

    TestDriver makeConnectionDriver(int db_port, String database_base_directory_path, String database_name, String username, String password, Set<Connection> connections_to_be_closed);

    TestDriver makeConnectionDriver(String database_name, String username, String password, Set<Connection> connections_to_be_closed);
}
