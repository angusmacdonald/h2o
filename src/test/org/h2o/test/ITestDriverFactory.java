package org.h2o.test;

import java.sql.Connection;
import java.util.Set;

public interface ITestDriverFactory {

    TestDriver makeConnectionDriver(int db_port, String string, String databaseName, String userName, String password, Set<Connection> connections_to_be_closed);

}
