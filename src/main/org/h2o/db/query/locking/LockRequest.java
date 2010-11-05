package org.h2o.db.query.locking;

import java.io.Serializable;

import org.h2.engine.Session;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

public class LockRequest implements Serializable {

    private static final long serialVersionUID = 468660463533445063L;

    private final DatabaseInstanceWrapper databaseMakingRequest;
    private final int sessionID;

    /**
     * 
     * @param databaseMakingRequest
     * @param sessionID                 This should be the value from session.getSerialID().
     */
    public LockRequest(final DatabaseInstanceWrapper databaseMakingRequest, final int sessionID) {

        this.databaseMakingRequest = databaseMakingRequest;
        this.sessionID = sessionID;
    }

    public static LockRequest createNewLockRequest(final Session session) {

        return new LockRequest(session.getDatabase().getLocalDatabaseInstanceInWrapper(), session.getSerialID());
    }

    public DatabaseInstanceWrapper getRequestLocation() {

        return databaseMakingRequest;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + (databaseMakingRequest == null ? 0 : databaseMakingRequest.hashCode());
        result = prime * result + sessionID;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {

        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (getClass() != obj.getClass()) { return false; }
        final LockRequest other = (LockRequest) obj;
        if (databaseMakingRequest == null) {
            if (other.databaseMakingRequest != null) { return false; }
        }
        else if (!databaseMakingRequest.equals(other.databaseMakingRequest)) { return false; }
        if (sessionID != other.sessionID) { return false; }
        return true;
    }

    @Override
    public String toString() {

        return "LockRequest [databaseMakingRequest=" + databaseMakingRequest + ", sessionID=" + sessionID + "]";
    }

}
