package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SchemaManager;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Table;

/**
 * Represents the DROP REPLICA command, allowing individual replicas to be dropped.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DropReplica extends SchemaCommand {
	//Mix of DropTable and CreateReplica code - the latter needed for the 'push' functionality.


	private boolean ifExists;
	private String tableName;
	private Table table;
	private DropReplica next;

	public DropReplica(Session session, Schema schema) {
		super(session, schema);
	}

	/**
	 * Chain another drop table statement to this statement.
	 *
	 * @param drop the statement to add
	 */
	public void addNextDropTable(DropReplica drop) {
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
		table = getSchema().findLocalTableOrView(session, tableName);

		if (table == null) {
			if (!ifExists) {
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
			}
		} else {
			session.getUser().checkRight(table, Right.ALL);
			if (!table.canDrop() || (Constants.IS_H2O && tableName.startsWith("H2O_"))) { //H20 - ensure schema tables aren't dropped.
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
			}
			int numberOfReplicas = SchemaManager.getInstance(session).getNumberofReplicas(tableName);
			
			if (numberOfReplicas == 1){ //can't drop the only replica.
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
			}
			table.lock(session, true, true);
		}
		if (next != null) {
			next.prepareDrop();
		}
	}

	private void executeDrop() throws SQLException {
		// need to get the table again, because it may be dropped already
		// meanwhile (dependent object, or same object)
		table = getSchema().findLocalTableOrView(session, tableName);
		if (table != null) {
			table.setModified();
			Database db = session.getDatabase();
			db.removeSchemaObject(session, table);


			/*
			 * #########################################################################
			 * 
			 *  Remove any schema manager entries.
			 * 
			 * #########################################################################
			 */
			if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_")){
				SchemaManager sm = SchemaManager.getInstance(session); //db.getSystemSession()
				sm.removeReplica(tableName, db.getDatabaseLocation(), db.getLocalMachineAddress(), db.getLocalMachinePort(), db.getConnectionType());
			}

		}
		if (next != null) {
			next.executeDrop();
		}
	}

	public int update() throws SQLException {
		session.commit(true);
		prepareDrop();
		executeDrop();
		return 0;
	}

}
