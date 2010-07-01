package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.h2o.comms.remote.TableManagerRemote;
import org.h2.h2o.manager.ISystemTable;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.util.TableInfo;
import org.h2.message.Message;
import org.h2.schema.Schema;

/**
 * Represents the DROP REPLICA command, allowing individual replicas to be dropped.
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class DropReplica extends SchemaCommand {
	//Mix of DropTable and CreateReplica code - the latter needed for the 'push' functionality.


	private boolean ifExists;
	private String tableName;
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
			if (!table.canDrop()) { //H2O - ensure schema tables aren't dropped. 
				throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
			}

			if (!table.getName().startsWith("H2O_")){

				int numberOfReplicas = 0;

				try {
					numberOfReplicas = session.getDatabase().getSystemTable().lookup(new TableInfo(tableName, getSchema().getName())).getTableManager().getNumberofReplicas();
				} catch (RemoteException e) {
					throw new SQLException("Failed in communication with the System Table.");
				} catch (MovedException e){
					throw new SQLException("System Table has moved.");
				}

				if (numberOfReplicas == 1){ //can't drop the only replica.
					throw Message.getSQLException(ErrorCode.CANNOT_DROP_TABLE_1, tableName);
				}

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
			 *  Remove any System Table entries.
			 * 
			 * #########################################################################
			 */
			if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_")){
				ISystemTable sm = db.getSystemTable(); //db.getSystemSession()

				TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), 0, table.getTableType(), db.getURL());
				
				try {
					TableManagerRemote tmr = sm.lookup(ti).getTableManager();
					tmr.removeReplicaInformation(ti);
				} catch (RemoteException e) {
					throw new SQLException("Failed to remove replica on System Table/Table Manager");
				} catch (MovedException e){
					throw new SQLException("System Table has moved.");
				}
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
