/***************************************************************************
 *                                                                         *
 * H2O                                                                     *
 * Copyright (C) 2010 Distributed Systems Architecture Research Group      *
 * University of St Andrews, Scotland                                      *
 * http://blogs.cs.st-andrews.ac.uk/h2o/                                   *
 *                                                                         *
 * This file is part of H2O, a distributed database based on the open      *
 * source database H2 (www.h2database.com).                                *
 *                                                                         *
 * H2O is free software: you can redistribute it and/or                    *
 * modify it under the terms of the GNU General Public License as          *
 * published by the Free Software Foundation, either version 3 of the      *
 * License, or (at your option) any later version.                         *
 *                                                                         *
 * H2O is distributed in the hope that it will be useful,                  *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of          *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the           *
 * GNU General Public License for more details.                            *
 *                                                                         *
 * You should have received a copy of the GNU General Public License       *
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.            *
 *                                                                         *
 ***************************************************************************/

package org.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.h2.engine.Constants;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.Message;
import org.h2.message.TraceSystem;

/**
 * The database driver. An application should not use this class directly. The only thing the application needs to do is load the driver.
 * This can be done using Class.forName. To load the driver and open a database connection, use the following code:
 * 
 * <pre>
 * Class.forName(&quot;org.h2.Driver&quot;);
 * Connection conn = DriverManager.getConnection(&quot;jdbc:h2:&tilde;/test&quot;, &quot;sa&quot;, &quot;sa&quot;);
 * </pre>
 */
public class Driver implements java.sql.Driver {

    private static final Driver INSTANCE = new Driver();

    private static volatile boolean registered;

    static {
        load();
    }

    /**
     * Open a database connection. This method should not be called by an application. Instead, the method DriverManager.getConnection
     * should be used.
     * 
     * @param url
     *            the database URL
     * @param info
     *            the connection properties
     * @return the new connection
     */
    @Override
    public Connection connect(final String url, Properties info) throws SQLException {

        try {
            if (info == null) {
                info = new Properties();
            }
            if (!acceptsURL(url)) { return null; }
            return new JdbcConnection(url, info);
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
    }

    /**
     * Check if the driver understands this URL. This method should not be called by an application.
     * 
     * @param url
     *            the database URL
     * @return if the driver understands the URL
     */
    @Override
    public boolean acceptsURL(final String url) {

        return url != null && url.startsWith(Constants.START_URL);
    }

    /**
     * Get the major version number of the driver. This method should not be called by an application.
     * 
     * @return the major version number
     */
    @Override
    public int getMajorVersion() {

        return Constants.VERSION_MAJOR;
    }

    /**
     * Get the minor version number of the driver. This method should not be called by an application.
     * 
     * @return the minor version number
     */
    @Override
    public int getMinorVersion() {

        return Constants.VERSION_MINOR;
    }

    /**
     * Get the list of supported properties. This method should not be called by an application.
     * 
     * @param url
     *            the database URL
     * @param info
     *            the connection properties
     * @return a zero length array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {

        return new DriverPropertyInfo[0];
    }

    /**
     * Check if this driver is compliant to the JDBC specification. This method should not be called by an application.
     * 
     * @return true
     */
    @Override
    public boolean jdbcCompliant() {

        return true;
    }

    /**
     * INTERNAL
     */
    public static synchronized Driver load() {

        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        }
        catch (final SQLException e) {
            TraceSystem.traceThrowable(e);
        }
        return INSTANCE;
    }

    /**
     * INTERNAL
     */
    public static synchronized void unload() {

        try {
            if (registered) {
                registered = false;
                DriverManager.deregisterDriver(INSTANCE);
            }
        }
        catch (final SQLException e) {
            TraceSystem.traceThrowable(e);
        }
    }

}
