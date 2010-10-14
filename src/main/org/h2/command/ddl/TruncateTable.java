/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement TRUNCATE TABLE
 */
public class TruncateTable extends DefineCommand {
	
	public TruncateTable(Session session) {
		super(session);
	}
	
	public int update() throws SQLException {
		session.commit(true);
		if ( !table.canTruncate() ) {
			throw Message.getSQLException(ErrorCode.CANNOT_TRUNCATE_1, table.getSQL());
		}
		session.getUser().checkRight(table, Right.DELETE);
		table.lock(session, true, true);
		table.truncate(session);
		return 0;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.h2.command.Prepared#acquireLocks()
	 */
	@Override
	public void acquireLocks(QueryProxyManager queryProxyManager) throws SQLException {
		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if ( isRegularTable() ) {
			
			QueryProxy queryProxy = queryProxyManager.getQueryProxy(table.getFullName());
			
			if ( queryProxy == null || !queryProxy.getLockGranted().equals(LockType.WRITE) ) {
				queryProxy = QueryProxy.getQueryProxyAndLock(table, LockType.WRITE, session.getDatabase());
			}
			
			queryProxyManager.addProxy(queryProxy);
		} else {
			queryProxyManager.addProxy(QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstanceInWrapper()));
		}
		
	}
	
}
