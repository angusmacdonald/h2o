/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.engine.Constants;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.h2o.util.LockType;
import org.h2.message.Message;
import org.h2.schema.Schema;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaCommand {

	private String constraintName;
	private boolean ifExists;
	private QueryProxy queryProxy = null;

	public AlterTableDropConstraint(Session session, Schema schema, boolean ifExists, boolean internalQuery) {
		super(session, schema);
		this.ifExists = ifExists;
		setInternalQuery(internalQuery);
	}

	public void setConstraintName(String string) {
		constraintName = string;
	}


	public int update(String transactionName) throws SQLException {
		session.commit(true);

		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()){
			return queryProxy.executeUpdate(sqlStatement, transactionName, session);
		}

		Constraint constraint = getSchema().findConstraint(session, constraintName);
		if (constraint == null) {
			if (!ifExists) {
				throw Message.getSQLException(ErrorCode.CONSTRAINT_NOT_FOUND_1, constraintName);
			}
		} else {
			session.getUser().checkRight(constraint.getTable(), Right.ALL);
			session.getUser().checkRight(constraint.getRefTable(), Right.ALL);
			session.getDatabase().removeSchemaObject(session, constraint);
		}
		return 0;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#acquireLocks()
	 */
	@Override
	public QueryProxy acquireLocks(QueryProxyManager queryProxyManager) throws SQLException {
		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()){

			queryProxy = queryProxyManager.getQueryProxy(getSchema().findConstraint(session, constraintName).getTable().getFullName());

			if (queryProxy == null){
				queryProxy = QueryProxy.getQueryProxyAndLock(getSchema().findConstraint(session, constraintName).getTable(), LockType.WRITE, session.getDatabase());
			}

			return queryProxy;
		}

		return QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstanceInWrapper());

	}

	/**
	 * True if the table involved in the prepared statement is a regular table - i.e. not an H2O meta-data table.
	 */
	protected boolean isRegularTable() {

		return Constants.IS_H2O && !session.getDatabase().isManagementDB() && !internalQuery;

	}
}
