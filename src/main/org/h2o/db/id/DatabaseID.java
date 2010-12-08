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

package org.h2o.db.id;

import java.io.Serializable;

/**
 * Unique identifier for an H2O database. This identifier is made up of:
 * <ul>
 * <li>Database URL: The JDBC address that allows other instances to connect to this H2O instance. Example: jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test </li>
 * <li>Database ID: A unique identifier used to represent this database. This ID will always be the same, while the database URL may change if the IP address of the
 * database changes.</li>
 * </ul>
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseID implements Serializable {

    private static final long serialVersionUID = 3202062668933786677L;

    /**
     * The name given to the database ID in H2O's properties files.
     */
    private static final String DATABASE_ID = "DATABASE_ID";

    /**
     * The unique ID of the database.
     */
    private String databaseID = null;

    /**
     * The current URL of the database.
     */
    private DatabaseURL databaseURL = null;

    public DatabaseID(final String databaseID, final DatabaseURL databaseURL) {

        this.databaseID = databaseID;
        this.databaseURL = databaseURL;
    }

    public DatabaseID(final String databaseID, final String databaseURL) {

        this.databaseID = databaseID;
        this.databaseURL = DatabaseURL.parseURL(databaseURL);
    }

    public static DatabaseID parseURL(final String url) {

        return new DatabaseID(null, DatabaseURL.parseURL(url));
    }

    public String getDatabaseID() {

        return databaseID;
    }

    public static void main(final String[] args) {

        // Test.
        System.out.println("First test, TCP DB:");
        DatabaseID dbID = DatabaseID.parseURL("jdbc:h2:sm:tcp://localhost:9081/db_data/unittests/schema_test");
        System.out.println(dbID.toString());

        System.out.println("\nSecond test, MEM DB:");
        dbID = DatabaseID.parseURL("jdbc:h2:sm:mem:one");
        System.out.println(dbID.toString());

        System.out.println("\nThird test, Other DB:");
        dbID = DatabaseID.parseURL("jdbc:h2:data/test/scriptSimple;LOG=1;LOCK_TIMEOUT=50");
        System.out.println(dbID.toString());

        System.out.println("\nFourth test, Tilde DB:");
        dbID = DatabaseID.parseURL("jdbc:h2:tcp://localhost/~/test");
        System.out.println(dbID.toString());

    }

    public String getURL() {

        return databaseURL.getURL();
    }

    public String getHostname() {

        return databaseURL.getHostname();
    }

    public int getPort() {

        return databaseURL.getPort();
    }

    public String getDbLocation() {

        return databaseURL.getDbLocation();
    }

    public String getPropertiesFilePath() {

        return databaseURL.getPropertiesFilePath();
    }

    public String sanitizedLocation() {

        return databaseURL.sanitizedLocation();
    }

    public boolean isMem() {

        return databaseURL.isMem();
    }

    public boolean isTcp() {

        return databaseURL.isTcp();
    }

    public boolean isSystemTable() {

        return databaseURL.isSystemTable();
    }

    public String getOriginalURL() {

        return databaseURL.getOriginalURL();
    }

    public boolean isValid() {

        return databaseURL.isValid();
    }

    public void setRMIPort(final int rmiPort) {

        databaseURL.setRMIPort(rmiPort);
    }

    public int getRMIPort() {

        return databaseURL.getRMIPort();
    }

    public String getConnectionType() {

        return databaseURL.getConnectionType();
    }

    public String getURLwithRMIPort() {

        return databaseURL.getURLwithRMIPort();
    }

    @Override
    public String toString() {

        return databaseURL.toString();
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (databaseURL == null ? 0 : databaseURL.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final DatabaseID other = (DatabaseID) obj;
        if (databaseURL == null) {
            if (other.databaseURL != null) { return false; }
        }
        else if (!databaseURL.equals(other.databaseURL)) { return false; }
        return true;
    }

}
