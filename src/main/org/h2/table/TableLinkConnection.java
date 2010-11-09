/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import org.h2.constant.SysProperties;
import org.h2.util.JdbcUtils;
import org.h2.util.ObjectUtils;
import org.h2.util.StringUtils;

/**
 * A connection for a linked table. The same connection may be used for multiple tables, that means a connection may be shared.
 */
public class TableLinkConnection {

    /**
     * The map where the link is kept.
     */
    private final HashMap<TableLinkConnection, TableLinkConnection> map;

    /**
     * The connection information.
     */
    private final String driver, url, user, password;

    /**
     * The database connection.
     */
    private Connection conn;

    /**
     * How many times the connection is used.
     */
    private int useCounter;

    private TableLinkConnection(final HashMap<TableLinkConnection, TableLinkConnection> map, final String driver, final String url, final String user, final String password) {

        this.map = map;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    /**
     * Open a new connection.
     * 
     * @param linkConnections
     *            the map where the connection should be stored (if shared connections are enabled).
     * @param driver
     *            the JDBC driver class name
     * @param url
     *            the database URL
     * @param user
     *            the user name
     * @param password
     *            the password
     * @return a connection
     */
    public static TableLinkConnection open(final HashMap<TableLinkConnection, TableLinkConnection> linkConnections, final String driver, final String url, final String user, final String password) throws SQLException {

        final TableLinkConnection t = new TableLinkConnection(linkConnections, driver, url, user, password);
        if (!SysProperties.SHARE_LINKED_CONNECTIONS) {
            t.open();
            return t;
        }
        synchronized (linkConnections) {
            TableLinkConnection result;
            result = linkConnections.get(t);
            if (result == null) {
                synchronized (t) {
                    t.open();
                }
                // put the connection in the map after is has been opened,
                // so we know it works
                linkConnections.put(t, t);
                result = t;
            }
            synchronized (result) {
                result.useCounter++;
            }
            return result;
        }
    }

    private void open() throws SQLException {

        conn = JdbcUtils.getConnection(driver, getUrl(), user, password);
    }

    @Override
    public int hashCode() {

        return ObjectUtils.hashCode(driver) ^ ObjectUtils.hashCode(getUrl()) ^ ObjectUtils.hashCode(user) ^ ObjectUtils.hashCode(password);
    }

    @Override
    public boolean equals(final Object o) {

        if (o instanceof TableLinkConnection) {
            final TableLinkConnection other = (TableLinkConnection) o;
            return StringUtils.equals(driver, other.driver) && StringUtils.equals(getUrl(), other.getUrl()) && StringUtils.equals(user, other.user) && StringUtils.equals(password, other.password);
        }
        return false;
    }

    /**
     * Get the connection. This method and methods on the statement must be synchronized on this object.
     * 
     * @return the connection
     */
    public Connection getConnection() {

        return conn;
    }

    /**
     * Closes the connection if this is the last link to it.
     */
    public synchronized void close() throws SQLException {

        if (--useCounter <= 0) {
            conn.close();
            conn = null;
            synchronized (map) {
                map.remove(this);
            }
        }
    }

    public String getUrl() {

        return url;
    }

}
