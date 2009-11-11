/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SchemaManager;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.h2o.comms.DataManager;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.util.LockType;
import org.h2.h2o.util.TransactionNameGenerator;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.DataType;

/**
 * This class represents the statement
 * CREATE TABLE
 */
public class CreateTable extends SchemaCommand {

	private String tableName;
	private ObjectArray constraintCommands = new ObjectArray();
	private ObjectArray columns = new ObjectArray();
	private IndexColumn[] pkColumns;
	private boolean ifNotExists;
	private boolean persistent = true;
	private boolean temporary;
	private boolean globalTemporary;
	private boolean onCommitDrop;
	private boolean onCommitTruncate;
	private Query asQuery;
	private String comment;
	private boolean clustered;
	private QueryProxy queryProxy = null;

	public CreateTable(Session session, Schema schema) {
		super(session, schema);
	}

	public void setQuery(Query query) {
		this.asQuery = query;
	}

	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Add a column to this table.
	 *
	 * @param column the column to add
	 */
	public void addColumn(Column column) {
		if (columns == null) {
			columns = new ObjectArray();
		}
		columns.add(column);
	}

	/**
	 * Add a constraint statement to this statement.
	 * The primary key definition is one possible constraint statement.
	 *
	 * @param command the statement to add
	 */
	public void addConstraintCommand(Prepared command) throws SQLException {
		if (command instanceof CreateIndex) {
			constraintCommands.add(command);
		} else {
			AlterTableAddConstraint con = (AlterTableAddConstraint) command;
			boolean alreadySet;
			if (con.getType() == AlterTableAddConstraint.PRIMARY_KEY) {
				alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
			} else {
				alreadySet = false;
			}
			if (!alreadySet) {
				constraintCommands.add(command);
			}
		}
	}

	public void setIfNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
	}

	public int update() throws SQLException {
		/*
		 * The only time this is called is when a CreateTable command is replayed at database startup. This differs
		 * from the normal CreateTable execution because a DataManager for the table may exist somewhere. Instead of creating a new
		 * data manager this command should look for an existing one somewhere. The command to create the Data Manager tables should have
		 * already been replayed, so the 
		 */
		return update(TransactionNameGenerator.generateName("NULLCREATION"));
	}

	public int update(String transactionName) throws SQLException {
		// TODO rights: what rights are required to create a table?
		session.commit(true);
		Database db = session.getDatabase();
		if (!db.isPersistent()) {
			persistent = false;
		}

		if ((getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE) != null && !isStartup()) || getSchema().findLocalTableOrView(session, tableName) != null) {
			if (ifNotExists) {
				return 0;
			}
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
		}




		if (asQuery != null) {
			asQuery.prepare();
			if (columns.size() == 0) {
				generateColumnsFromQuery();
			} else if (columns.size() != asQuery.getColumnCount()) {
				throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
			}
		}
		if (pkColumns != null) {
			int len = pkColumns.length;
			for (int i = 0; i < columns.size(); i++) {
				Column c = (Column) columns.get(i);
				for (int j = 0; j < len; j++) {
					if (c.getName().equals(pkColumns[j].columnName)) {
						c.setNullable(false);
					}
				}
			}
		}
		ObjectArray sequences = new ObjectArray();
		for (int i = 0; i < columns.size(); i++) {
			Column c = (Column) columns.get(i);
			if (c.getAutoIncrement()) {
				int objId = getObjectId(true, true);
				c.convertAutoIncrementToSequence(session, getSchema(), objId, temporary);
			}
			Sequence seq = c.getSequence();
			if (seq != null) {
				sequences.add(seq);
			}
		}
		int id = getObjectId(true, true);

		TableData table = getSchema().createTable(tableName, id, columns, persistent, clustered, headPos);
		table.setComment(comment);
		table.setTemporary(temporary);
		table.setGlobalTemporary(globalTemporary);
		if (temporary && !globalTemporary) {
			if (onCommitDrop) {
				table.setOnCommitDrop(true);
			}
			if (onCommitTruncate) {
				table.setOnCommitTruncate(true);
			}
			session.addLocalTempTable(table);
		} else {
			db.addSchemaObject(session, table);
		}
		try {
			for (int i = 0; i < columns.size(); i++) {
				Column c = (Column) columns.get(i);
				c.prepareExpression(session);
			}
			for (int i = 0; i < sequences.size(); i++) {
				Sequence sequence = (Sequence) sequences.get(i);
				table.addSequence(sequence);
			}
			for (int i = 0; i < constraintCommands.size(); i++) {
				Prepared command = (Prepared) constraintCommands.get(i);
				command.update();
			}
			if (asQuery != null) {
				boolean old = session.getUndoLogEnabled();
				try {
					session.setUndoLogEnabled(false);
					Insert insert = null;
					insert = new Insert(session, true);
					insert.setQuery(asQuery);
					insert.setTable(table);
					insert.prepare();
					insert.update();
				} finally {
					session.setUndoLogEnabled(old);
				}
			}


			/*
			 * #########################################################################
			 * 
			 *  Create a schema manager entry.
			 * 
			 * #########################################################################
			 */
			if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_")){
				SchemaManager sm = SchemaManager.getInstance(session); //db.getSystemSession()
				int tableSet = -1;
				boolean thisTableReferencesAnExistingTable = false;


				if (table.getConstraints() != null){
					Constraint[] constraints = new Constraint[table.getConstraints().size()];
					table.getConstraints().toArray(constraints);



					Set<Table> referencedTables = new HashSet<Table>();
					for (Constraint con: constraints){
						if (con instanceof ConstraintReferential){
							thisTableReferencesAnExistingTable = true;
							referencedTables.add(con.getRefTable());
						}
					}

					if (thisTableReferencesAnExistingTable){ 
						if (referencedTables.size() > 1){
							System.err.println("Unexpected. Test that this still works.");
						}
						for (Table tab: referencedTables){
							tableSet = tab.getTableSet();
						}
					} else {
						tableSet = sm.getNewTableSetNumber();
					}
				} else {
					tableSet = sm.getNewTableSetNumber();
				}
				sm.addTableInformation(tableName, table.getModificationId(), db.getDatabaseLocation(), table.getTableType(), 
						db.getLocalMachineAddress(), db.getLocalMachinePort(), (db.isPersistent())? "tcp": "mem", getSchema().getName(), tableSet, session);	

				table.setTableSet(tableSet);

				/*
				 * (add replicas at some external locations).
				 */
				//				assert(queryProxy!= null);

				assert(db != null);

				//				if (Constants.IS_H2O && !table.getName().startsWith(Constants.H2O_SCHEMA) && !session.getDatabase().isManagementDB() && 
				//						!queryProxy.isSingleDatabase(db.getLocalDatabaseInstance()) ){
				//					return queryProxy.executeUpdate("CREATE REPLICA " + table.getFullName(), transactionName);
				//				}

			}

			if (Constants.IS_H2O && !db.isManagementDB()){
				prepareTransaction(transactionName);
			}

		} catch (SQLException e) {
			db.checkPowerOff();
			db.removeSchemaObject(session, table);
			throw e;
		}

		return 0;
	}

	private void generateColumnsFromQuery() {
		int columnCount = asQuery.getColumnCount();
		ObjectArray expressions = asQuery.getExpressions();
		for (int i = 0; i < columnCount; i++) {
			Expression expr = (Expression) expressions.get(i);
			int type = expr.getType();
			String name = expr.getAlias();
			long precision = expr.getPrecision();
			int displaySize = expr.getDisplaySize();
			DataType dt = DataType.getDataType(type);
			if (precision > 0 && (dt.defaultPrecision == 0 || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
				// dont' set precision to MAX_VALUE if this is the default
				precision = dt.defaultPrecision;
			}
			int scale = expr.getScale();
			if (scale > 0 && (dt.defaultScale == 0 || dt.defaultScale > scale)) {
				scale = dt.defaultScale;
			}
			Column col = new Column(name, type, precision, scale, displaySize);
			addColumn(col);
		}
	}

	/**
	 * Sets the primary key columns, but also check if a primary key
	 * with different columns is already defined.
	 *
	 * @param columns the primary key columns
	 * @return true if the same primary key columns where already set
	 */
	private boolean setPrimaryKeyColumns(IndexColumn[] columns) throws SQLException {
		if (pkColumns != null) {
			if (columns.length != pkColumns.length) {
				throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
			}
			for (int i = 0; i < columns.length; i++) {
				if (!columns[i].columnName.equals(pkColumns[i].columnName)) {
					throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
				}
			}
			return true;
		}
		this.pkColumns = columns;
		return false;
	}

	public void setPersistent(boolean persistent) {
		this.persistent = persistent;
	}

	public void setGlobalTemporary(boolean globalTemporary) {
		this.globalTemporary = globalTemporary;
	}

	/**
	 * This temporary table is dropped on commit.
	 */
	public void setOnCommitDrop() {
		this.onCommitDrop = true;
	}

	/**
	 * This temporary table is truncated on commit.
	 */
	public void setOnCommitTruncate() {
		this.onCommitTruncate = true;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public void setClustered(boolean clustered) {
		this.clustered = clustered;
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#shouldBePropagated()
	 */
	@Override
	public boolean shouldBePropagated() {
		/*
		 * If this is not a regular table (i.e. it is a meta-data table, then it will not be propagated regardless.
		 */
		return isRegularTable();
	}



	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#isRegularTable()
	 */
	@Override
	protected boolean isRegularTable() {
		return Constants.IS_H2O && !session.getDatabase().isManagementDB() && !internalQuery && !tableName.startsWith(Constants.H2O_SCHEMA);
	}

	/* (non-Javadoc)
	 * @see org.h2.command.Prepared#acquireLocks()
	 * 
	 * The queryProxyManager variable isn't used in create table, because it can't have a proxy for something
	 * which hasn't yet been created.
	 */
	@Override
	public QueryProxy acquireLocks(QueryProxyManager queryProxyManager) throws SQLException {
		Database db = session.getDatabase();

		assert queryProxyManager.getQueryProxy(tableName) == null; //should never exist.

		/*
		 * #########################################################################
		 * 
		 *  H2O. Check that the table doesn't already exist elsewhere.
		 * 
		 * #########################################################################
		 */

		if (Constants.IS_H2O && !db.isSM() && !db.isManagementDB() && !tableName.startsWith("H2O_") && !isStartup()){

			DataManagerRemote dm = db.getDataManager(getSchema().getName() + "." + tableName);

			if (dm != null){
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
			}


		}

		queryProxy = null;
		if (Constants.IS_H2O && !tableName.startsWith("H2O_") && !db.isManagementDB()){ //XXX Not sure if this should be a seperate IF
			queryProxy = QueryProxy.getQueryProxy(new DataManager(tableName, getSchema().getName(), 0, 0, db), LockType.CREATE, db.getLocalDatabaseInstance());

		} else if (Constants.IS_H2O){
			/*
			 * This is a system table, but it still needs a QueryProxy to indicate that it is acceptable to execute the query.
			 */
			queryProxy = QueryProxy.getQueryProxy(table, LockType.CREATE, db);

		}

		return queryProxy;
	}

}
