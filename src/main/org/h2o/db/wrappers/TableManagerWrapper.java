/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.wrappers;

import java.io.Serializable;

import org.h2o.db.id.DatabaseID;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.ITableManagerRemote;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TableManagerWrapper implements Serializable {

    private static final long serialVersionUID = -7088367740432999328L;

    /**
     * Contains information on the table itself, such as its fully qualified name.
     */
    private TableInfo tableInfo;

    /**
     * Remote reference to the actual Table Manager.
     */
    private ITableManagerRemote tableManager;

    /**
     * Location of the Table Manager.
     */
    private DatabaseID tableManagerURL;

    public TableManagerWrapper(final TableInfo tableInfo, final ITableManagerRemote tableManager, final DatabaseID tableManagerURL) {

        this.tableInfo = tableInfo.getGenericTableInfo();
        this.tableManager = tableManager;
        this.tableManagerURL = tableManagerURL;
    }

    /**
     * @return the tableInfo
     */
    public TableInfo getTableInfo() {

        return tableInfo;
    }

    /**
     * @param tableInfo
     *            the tableInfo to set
     */
    public void setTableInfo(final TableInfo tableInfo) {

        this.tableInfo = tableInfo.getGenericTableInfo();
    }

    /**
     * @return the tableManager
     */
    public ITableManagerRemote getTableManager() {

        return tableManager;
    }

    /**
     * @param tableManager
     *            the tableManager to set
     */
    public void setTableManager(final ITableManagerRemote tableManager) {

        this.tableManager = tableManager;
    }

    /**
     * @return the tableManagerURL
     */
    public DatabaseID getURL() {

        return tableManagerURL;
    }

    /**
     * @param tableManagerURL
     *            the tableManagerURL to set
     */
    public void setTableManagerURL(final DatabaseID tableManagerURL) throws RPCException {

        this.tableManagerURL = tableManagerURL;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {

        return tableInfo.hashCode();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final TableManagerWrapper other = (TableManagerWrapper) obj;
        if (tableInfo == null) {
            if (other.tableInfo != null) { return false; }
        }
        else if (!tableInfo.equals(other.tableInfo)) { return false; }
        return true;
    }

    /**
     * @param localMachineLocation
     * @return
     */
    public boolean isLocalTo(final DatabaseID localMachineLocation) {

        return tableManagerURL.equals(localMachineLocation);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "TableManagerWrapper [tableInfo=" + tableInfo + ", tableManagerURL=" + tableManagerURL + "]";
    }

}
