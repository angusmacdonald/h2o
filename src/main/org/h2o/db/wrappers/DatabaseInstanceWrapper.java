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

package org.h2o.db.wrappers;

import java.io.Serializable;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.interfaces.IDatabaseInstanceRemote;
import org.h2o.util.DatabaseInstanceProbability;

/**
 * Wrapper for remote database instance proxies. Contains a reference to the proxy itself and whether the database is actually alive.
 * 
 * <p>
 * This is done because connection information is maintained in the System Table even when a connection itself has become inactive.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DatabaseInstanceWrapper implements Serializable, Comparable<DatabaseInstanceWrapper> {

    private static final long serialVersionUID = 9193285872031823819L;

    private final DatabaseID databaseURL;

    private final IDatabaseInstanceRemote databaseInstance;

    private boolean active = true;

    private final DatabaseInstanceProbability availabilityInfo;

    /**
     * @param databaseURL
     *            The location of this database instance.
     * @param databaseInstance
     *            Reference to the local database instance.
     * @param active
     *            Whether the database is currently active.
     */
    public DatabaseInstanceWrapper(final DatabaseID databaseURL, final IDatabaseInstanceRemote databaseInstance, final boolean active) {

        super();
        this.databaseURL = databaseURL;
        this.databaseInstance = databaseInstance;
        this.active = active;
        availabilityInfo = new DatabaseInstanceProbability(0.5);
    }

    /**
     * Whether the database instance this proxy points to is still active.
     */
    public boolean isActive() {

        return active;
    }

    /**
     * Set the database instance as active (allowing incoming connections) or inactive (not running).
     */
    public void setActive(final boolean active) {

        this.active = active;
    }

    /**
     * @return Remote proxy for a database instances interface.
     */
    public IDatabaseInstanceRemote getDatabaseInstance() {

        return databaseInstance;
    }

    public DatabaseID getURL() {

        return databaseURL;
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
        final DatabaseInstanceWrapper other = (DatabaseInstanceWrapper) obj;
        if (databaseURL == null) {
            if (other.databaseURL != null) { return false; }
        }
        else if (!databaseURL.equals(other.databaseURL)) { return false; }
        return true;
    }

    public DatabaseInstanceProbability getAvailabilityInfo() {

        return availabilityInfo;
    }

    public boolean getActive() {

        return active;
    }

    @Override
    public int compareTo(final DatabaseInstanceWrapper o) {

        return getAvailabilityInfo().compareTo(o.getAvailabilityInfo());
    }

    @Override
    public String toString() {

        return "DatabaseInstanceWrapper [databaseURL=" + databaseURL + "]";
    }
}
