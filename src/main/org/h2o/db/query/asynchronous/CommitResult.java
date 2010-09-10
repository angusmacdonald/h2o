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
	 * @param expectedUpdateID
	 * @param tableName
	 *            Name of the table which is being updated by this query.
	 */
	public CommitResult(boolean commit, DatabaseInstanceWrapper wrapper, int updateID, int expectedUpdateID, TableInfo tableName) {
		this.commit = commit;
		this.wrapper = wrapper;
		this.updateID = updateID;
		this.expectedUpdateID = expectedUpdateID;
		this.tableName = tableName;

		if (tableName == null){
			this.isCommitQuery = true;
		} else {
			this.isCommitQuery = false;
		}
	}

	public CommitResult(boolean commit, DatabaseInstanceWrapper wrapper, int updateID, int expectedUpdateID) {
		this.commit = commit;
		this.wrapper = wrapper;
		this.updateID = updateID;
		this.expectedUpdateID = expectedUpdateID;
		this.isCommitQuery = true;
		this.tableName = null;
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
		result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
		result = prime * result + updateID;
		result = prime * result + ((wrapper == null) ? 0 : wrapper.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CommitResult other = (CommitResult) obj;
		if (commit != other.commit)
			return false;
		if (tableName == null) {
			if (other.tableName != null)
				return false;
		} else if (!tableName.equals(other.tableName))
			return false;
		if (updateID != other.updateID)
			return false;
		if (wrapper == null) {
			if (other.wrapper != null)
				return false;
		} else if (!wrapper.equals(other.wrapper))
			return false;
		return true;
	}

	

	@Override
	public String toString() {
		return "CommitResult [wrapper=" + wrapper + ", commit=" + commit + ", updateID=" + updateID + ", expectedUpdateID="
				+ expectedUpdateID + ", tableName=" + tableName + ", isCommitQuery=" + isCommitQuery + "]";
	}

	public boolean isCommitQuery() {
		return isCommitQuery;
	}


}
