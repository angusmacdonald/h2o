/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2o.db.query.TableProxyManager;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaCommand {

    private String constraintName;

    private final boolean ifExists;

    public AlterTableDropConstraint(final Session session, final Schema schema, final boolean ifExists, final boolean internalQuery) {

        super(session, schema);
        this.ifExists = ifExists;
        setInternalQuery(internalQuery);
    }

    public void setConstraintName(final String string) {

        constraintName = string;
    }

    @Override
    public int update(final String transactionName) throws SQLException {

        session.commit(true);

        /*
         * (QUERY PROPAGATED TO ALL REPLICAS).
         */
        if (isRegularTable()) { return tableProxy.executeUpdate(sqlStatement, transactionName, session); }

        final Constraint constraint = getSchema().findConstraint(session, constraintName);
        if (constraint == null) {
            if (!ifExists) { throw Message.getSQLException(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName); }
        }
        else {
            session.getUser().checkRight(constraint.getTable(), Right.ALL);
            session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
            session.getDatabase().removeSchemaObject(session, constraint);
        }
        return 0;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.command.Prepared#acquireLocks()
     */
    @Override
    public void acquireLocks(final TableProxyManager tableProxyManager) throws SQLException {

        final Table nonNullTable = getSchema().findConstraint(session, constraintName).getTable();
        acquireLocks(tableProxyManager, nonNullTable, LockType.WRITE);

    }

    /**
     * True if the table involved in the prepared statement is a regular table - i.e. not an H2O meta-data table.
     */
    @Override
    protected boolean isRegularTable() {

        return !session.getDatabase().isManagementDB() && !internalQuery;

    }
}
