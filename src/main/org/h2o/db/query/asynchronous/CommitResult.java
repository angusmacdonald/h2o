package org.h2o.db.query.asynchronous;

import java.io.Serializable;

import org.h2o.db.id.DatabaseURL;

public class CommitResult implements Serializable {

	private static final long serialVersionUID = 7332399392218826479L;

	private final DatabaseURL url;
	private final boolean commit;
	private final int updateID;

	/**
	 * 
	 * @param commit	True if the replica is ready to be committed on the remote machine.
	 * @param url		The URL of the machine which executed this update.
	 * @param updateID	The update ID corresponding to this update.
	 */
	public CommitResult(boolean commit, DatabaseURL url, int updateID) {
		this.commit = commit;
		this.url = url;
		this.updateID = updateID;
	}

	public DatabaseURL getUrl() {
		return url;
	}

	public boolean isCommit() {
		return commit;
	}

	public int getUpdateID() {
		return updateID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + updateID;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
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
		if (updateID != other.updateID)
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

	
}
