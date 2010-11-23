package org.h2o.db.query.locking;

import java.sql.SQLException;

public class LockException extends SQLException {

    public LockException(final String reason) {

        super(reason);
    }
}
