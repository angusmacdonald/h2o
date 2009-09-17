/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SchemaManager;
import org.h2.engine.Session;
import org.h2.h2o.comms.QueryProxy;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;

/**
 * This class represents the statement
 * DROP TABLE
 */
public class DropTable extends SchemaCommand {

	private boolean ifExists;
	private String tableName;
	private Table table = null;
	ReplicaSet tables = null;
	private DropTable next;

	private boolean internalQuery;

	public DropTable(Session session, Schema schema, boolean internalQuery) {
		super(session, schema);

		this.internalQuery = internalQuery;
	}

	/**
	 * Chain another drop table statement to this statement.
	 *
	 * @param drop the statement to add
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

	private void prepareDrop() throws SQLException {

		if (Constants.IS_H2O){
			tables = getSchema().getTablesOrViews(session, tableName);

			if (tables != null){
				table = tables.getACopy();
			}
		} else {
			table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);
		}
		//		DataManagerRemote dm = null;
		//
		//		if (Constants.IS_H2O){
		//			dm = getSchema().getDatabase().getDataManager(getSchema().getName() + "." + tableName);
		//		}

		// TODO drop table: drops views as well (is this ok?)
		if (table == null) {
			//table = null;
			if (!ifExists) {
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
			}
		} else {
			session.getUser().checkRight(table, Right.ALL);
			if (!table.canDrop() || (Constants.IS_H2O && tableName.startsWith("H2O_"))) { //H2O - ensure schema tables aren't dropped.
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
			}
			if (Constants.IS_H2O && !internalQuery){
				table.lock(session, true, true); //lock isn't acquired here - the query is distributed to each replica first.
			}
		}
		if (next != null) {
			next.prepareDrop();
		}
	}

	private void executeDrop() throws SQLException {
		// need to get the table again, because it may be dropped already
		// meanwhile (dependent object, or same object)
		table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);
		if (table != null) {
			Database db = session.getDatabase();
			String fullTableName = getSchema().getName() + "." + tableName;

			/*
			 * #########################################################################
			 * 
			 *  Remove any schema manager entries.
			 * 
			 * #########################################################################
			 */
			if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_") && !internalQuery){

				QueryProxy qp = QueryProxy.getQueryProxy(session.getDatabase().getDataManager(table.getSchema().getName() + "." + table.getName()));
				qp.executeUpdate(sqlStatement);

				try {
					SchemaManager sm = SchemaManager.getInstance(session); //db.getSystemSession()
					sm.removeTable(tableName, getSchema().getName());

					db.removeDataManager(fullTableName, false);
				} catch (SQLException e){
					//TODO fix - this is thrown because the tableID is not found. This happens because drop table removes only local copies AND
					// schema manager information.
				}

			} else {

				if (Constants.IS_H2O){
					/*
					 * We want to remove the local copy of the data plus any other references to remote tables (i.e. linked tables need to
					 * be removed as well) 
					 */
					Table[] tableArray = tables.getAllCopies().toArray(new Table[0]); // add to array to prevent concurrent modification exceptions.

					for (Table t: tableArray){
						t.setModified();
						db.removeSchemaObject(session, t);
					}

					db.removeDataManager(fullTableName, true);
				} else {
					// Default H2 behaviour.
					table.setModified();
					db.removeSchemaObject(session, table);

				}

			}

		}
		if (next != null) {
			next.executeDrop();
		}
	}

	@Override
	public int update() throws SQLException {
		session.commit(true);
		prepareDrop();
		executeDrop();
		return 0;
	}

}
