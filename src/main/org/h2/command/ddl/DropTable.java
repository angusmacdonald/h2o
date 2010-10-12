/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockType;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.client.DatabaseStates;
import org.h2o.viewer.client.H2OEvent;
import org.h2o.viewer.client.H2OEventBus;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * This class represents the statement DROP TABLE
 */
public class DropTable extends SchemaCommand {

	private boolean ifExists;
	private String tableName;
	ReplicaSet tables = null;
	private DropTable next;
	private QueryProxy queryProxy;

	public DropTable(Session session, Schema schema, boolean internalQuery) {
		super(session, schema);

		this.internalQuery = internalQuery;
	}

	/**
	 * Chain another drop table statement to this statement.
	 * 
	 * @param drop
	 *            the statement to add
	 */
	public void addNextDropTable(DropTable drop) {
		if (next == null) {
			next = drop;
		} else {
			next.addNextDropTable(drop);
		}
	}

	public void setIfExists(boolean b) {
		ifExists = b;
		if (next != null) {
			next.setIfExists(b);
		}
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	private void prepareDrop(String transactionName) throws SQLException {

		if (Constants.IS_H2O) {
			tables = getSchema().getTablesOrViews(session, tableName);

			if (tables != null) {
				table = tables.getACopy();
			}
		} else {
			table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);
		}

		TableManagerRemote tableManager = null;
		if (table == null) {
			tableManager = getSchema().getDatabase().getSystemTableReference().lookup((getSchema().getName() + "." + tableName), false);
		}

		if (table == null && tableManager == null) {
			// table = null;
			if (!ifExists) {
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
			}
		} else {
			session.getUser().checkRight(table, Right.ALL);

			// XXX changed to add the table null checks because some tests show
			// up the tableManager existing when the local table doesn't.
			if ((table != null && !table.canDrop()) || (Constants.IS_H2O && tableName.startsWith("H2O_"))) { // H2O
				// -
				// ensure
				// schema
				// tables
				// aren't
				// dropped.
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
			}
			if (Constants.IS_H2O && !internalQuery && table != null) {
				table.lock(session, true, true); // lock isn't acquired here -
				// the query is distributed
				// to each replica first.
			}
		}
		if (next != null) {
			next.prepareDrop(transactionName);
		}
	}

	private void executeDrop(String transactionName) throws SQLException, RemoteException {
		// need to get the table again, because it may be dropped already
		// meanwhile (dependent object, or same object)
		table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);
		if (table != null) {
			Database db = session.getDatabase();
			/*
			 * ################################################################## #######
			 * 
			 * Remove any System Table entries.
			 * 
			 * ################################################################## #######
			 */

			if (Constants.IS_H2O && !db.isManagementDB() && !db.isTableLocal(getSchema()) && !internalQuery) {

				queryProxy.executeUpdate(sqlStatement, transactionName, session);

				ISystemTableReference sm = db.getSystemTableReference();
				try {
					sm.removeTableInformation(new TableInfo(tableName, getSchema().getName()));
				} catch (MovedException e) {
					db.getSystemTableReference().handleMovedException(e);

					try {
						sm.removeTableInformation(new TableInfo(tableName, getSchema().getName()));
					} catch (MovedException e1) {
						throw new RemoteException("System Table has moved.");
					}
				}

			} else { // It's an internal query...

				/*
				 * We want to remove the local copy of the data plus any other references to remote tables (i.e. linked tables need to be
				 * removed as well)
				 */
				Table[] tableArray = tables.getAllCopies().toArray(new Table[0]); // add to array to prevent concurrent
				// modification exceptions.

				for (Table t : tableArray) {
					t.setModified();
					db.removeSchemaObject(session, t);
				}

				// db.removeTableManager(fullTableName, true);

			}
			H2OEventBus.publish(new H2OEvent(db.getURL().getDbLocation(), DatabaseStates.TABLE_DELETION, getSchema().getName() + "." + tableName));
		}
		if (next != null) {
			next.executeDrop(transactionName);
		}
	}

	@Override
	public int update(String transactionName) throws SQLException, RemoteException {
		session.commit(true);
		prepareDrop(transactionName);
		executeDrop(transactionName);
		return 0;
	}

	@Override
	public int update() throws SQLException, RemoteException {
		String transactionName = "None";

		session.commit(true);
		prepareDrop(transactionName);
		executeDrop(transactionName);
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#acquireLocks()
	 */
	@Override
	public void acquireLocks(QueryProxyManager queryProxyManager) throws SQLException {
		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()) {

			String fullTableName = getSchema().getName() + "." + tableName;
			queryProxy = queryProxyManager.getQueryProxy(fullTableName);

			if (queryProxy == null || !queryProxy.getLockGranted().equals(LockType.WRITE)) {

				Database database = this.getSchema().getDatabase();
				TableManagerRemote tableManager = database.getSystemTableReference().lookup(fullTableName, true);


				if (tableManager == null){
					//Will happen if the table doesn't exist but IF NOT EXISTS has been specified.
					queryProxy = QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstanceInWrapper());
				} else {
					/*
					 * A DROP lock is requested if auto-commit is off (so that the update ID returned is 0), but a WRITE lock is given. If auto-commit
					 * is on then no other queries can come in after the drop request so the write lock is sufficient.
					 */
					
					LockType lockToRequest = (session.getApplicationAutoCommit())? LockType.WRITE: LockType.DROP;
					
					queryProxy = QueryProxy.getQueryProxyAndLock(tableManager, fullTableName, database, lockToRequest, session.getDatabase()
							.getLocalDatabaseInstanceInWrapper(), false);
				}
			}
			queryProxyManager.addProxy(queryProxy);
		} else {
			queryProxyManager.addProxy(QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstanceInWrapper()));
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#isRegularTable()
	 */
	@Override
	protected boolean isRegularTable() {
		Set<String> localSchema = session.getDatabase().getLocalSchema();
		try {
			return Constants.IS_H2O && !session.getDatabase().isManagementDB() && !internalQuery
			&& !localSchema.contains(getSchema().getName());
		} catch (NullPointerException e) {
			// Shouldn't occur, ever. Something should have probably overridden
			// this if it can't possibly know about a particular table.
			ErrorHandling.hardError("isRegularTable() check failed.");
			return false;
		}
	}

}
