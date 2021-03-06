/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved. Project Homepage:
 * http://blogs.cs.st-andrews.ac.uk/h2o H2O is free software: you can redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. H2O
 * is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU General
 * Public License along with H2O. If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.io.Serializable;

import org.h2o.db.id.TableInfo;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public class CommitResult implements Serializable {

    private static final long serialVersionUID = 7332399392218826479L;

    private final DatabaseInstanceWrapper wrapper;

    private final boolean commit;

    private final int updateID;

    private final int expectedUpdateID;

    private final TableInfo tableName;

    private final boolean isCommitQuery;

    /**
     * 
     * @param commit
     *            True if the replica is ready to be committed on the remote machine.
     * @param wrapper
     *            The URL of the machine which executed this update.
     * @param updateID
     *            The update ID corresponding to this update.
     * @param expectedUpdateID
     *            The update ID that the replica should match when it commits to the table manager.
     * @param tableName
     *            Name of the table which is being updated by this query.
     */
    public CommitResult(final boolean commit, final DatabaseInstanceWrapper wrapper, final int updateID, final int expectedUpdateID, final TableInfo tableName) {

        this.commit = commit;
        this.wrapper = wrapper;
        this.updateID = updateID;
        this.expectedUpdateID = expectedUpdateID;
        this.tableName = tableName;

        isCommitQuery = tableName == null;
    }

    public CommitResult(final boolean commit, final DatabaseInstanceWrapper wrapper, final int updateID, final int expectedUpdateID) {

        this.commit = commit;
        this.wrapper = wrapper;
        this.updateID = updateID;
        this.expectedUpdateID = expectedUpdateID;
        isCommitQuery = true;
        tableName = null;
    }

    public DatabaseInstanceWrapper getDatabaseInstanceWrapper() {

        return wrapper;
    }

    public boolean isCommit() {

        return commit;
    }

    public int getUpdateID() {

        return updateID;
    }

    public int getExpectedUpdateID() {

        return expectedUpdateID;
    }

    public TableInfo getTable() {

        return tableName;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (commit ? 1231 : 1237);
        result = prime * result + (tableName == null ? 0 : tableName.hashCode());
        result = prime * result + updateID;
        result = prime * result + (wrapper == null ? 0 : wrapper.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final CommitResult other = (CommitResult) obj;
        if (commit != other.commit) { return false; }
        if (tableName == null) {
            if (other.tableName != null) { return false; }
        }
        else if (!tableName.equals(other.tableName)) { return false; }
        if (updateID != other.updateID) { return false; }
        if (wrapper == null) {
            if (other.wrapper != null) { return false; }
        }
        else if (!wrapper.equals(other.wrapper)) { return false; }
        return true;
    }

    @Override
    public String toString() {

        return "CommitResult [wrapper=" + wrapper + ", commit=" + commit + ", updateID=" + updateID + ", expectedUpdateID=" + expectedUpdateID + ", tableName=" + tableName + ", isCommitQuery=" + isCommitQuery + "]";
    }

    public boolean isCommitQuery() {

        return isCommitQuery;
    }

}
