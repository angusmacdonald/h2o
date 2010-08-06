/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.HashSet;

import org.h2.constant.ErrorCode;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintCheck;
import org.h2.constraint.ConstraintReferential;
import org.h2.constraint.ConstraintUnique;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.h2o.util.LockType;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * ALTER TABLE ADD CONSTRAINT
 */
public class AlterTableAddConstraint extends SchemaCommand {

	/**
	 * The type of a ALTER TABLE ADD CHECK statement.
	 */
	public static final int CHECK = 0;

	/**
	 * The type of a ALTER TABLE ADD UNIQUE statement.
	 */
	public static final int UNIQUE = 1;

	/**
	 * The type of a ALTER TABLE ADD FOREIGN KEY statement.
	 */
	public static final int REFERENTIAL = 2;

	/**
	 * The type of a ALTER TABLE ADD PRIMARY KEY statement.
	 */
	public static final int PRIMARY_KEY = 3;

	private int type;
	private String constraintName;
	private String tableName;
	private IndexColumn[] indexColumns;
	private int deleteAction;
	private int updateAction;
	private Schema refSchema;
	private String refTableName;
	private IndexColumn[] refIndexColumns;
	private Expression checkExpression;
	private Index index, refIndex;
	private String comment;
	private boolean checkExisting;
	private boolean primaryKeyHash;
	private boolean ifNotExists;

	private QueryProxy queryProxy = null;

	public AlterTableAddConstraint(Session session, Schema schema, boolean ifNotExists, boolean internalQuery) {
		super(session, schema);
		this.ifNotExists = ifNotExists;
		setInternalQuery(internalQuery);
	}

	private String generateConstraintName(Table table) {
		if (constraintName == null) {
			constraintName = getSchema().getUniqueConstraintName(session, table);
		}
		return constraintName;
	}

	public int update() throws SQLException {
		return update("AddConstraint");
	}

	public int update(String transactionName) throws SQLException {
		try {
			return tryUpdate(transactionName);
		} finally {
			getSchema().freeUniqueName(constraintName);
		}
	}

	/**
	 * Try to execute the statement.
	 *
	 * @return the update count
	 */
	public int tryUpdate(String transactionName) throws SQLException {
		session.commit(true);

		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()){

			//			if (queryProxy.getNumberOfReplicas() > 1){
			if (sqlStatement != null){

				if (queryProxy == null){
					queryProxy = QueryProxy.getQueryProxyAndLock(getSchema().getTableOrView(session, tableName), LockType.WRITE, session.getDatabase());
				}
				return queryProxy.executeUpdate(sqlStatement, transactionName, session);
			} 
			//			} //Else, just execute it now.
		}

		Database db = session.getDatabase();
		Table table = getSchema().getTableOrView(session, tableName);
		if (getSchema().findConstraint(session, constraintName) != null) {
			if (ifNotExists || isStartup()) {
				return 0;
			}
			throw Message.getSQLException(ErrorCode.CONSTRAINT_ALREADY_EXISTS_1, constraintName);
		}

		session.getUser().checkRight(table, Right.ALL);
		table.lock(session, true, true);
		Constraint constraint;
		switch (type) {
		case PRIMARY_KEY: {
			IndexColumn.mapColumns(indexColumns, table);
			index = table.findPrimaryKey();
			ObjectArray constraints = table.getConstraints();
			for (int i = 0; constraints != null && i < constraints.size(); i++) {
				Constraint c = (Constraint) constraints.get(i);
				if (Constraint.PRIMARY_KEY.equals(c.getConstraintType())) {
					throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
				}
			}
			if (index != null) {
				// if there is an index, it must match with the one declared
				// we don't test ascending / descending
				IndexColumn[] pkCols = index.getIndexColumns();
				if (pkCols.length != indexColumns.length) {
					throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
				}
				for (int i = 0; i < pkCols.length; i++) {
					if (pkCols[i].column != indexColumns[i].column) {
						throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY);
					}
				}
			}
			if (index == null) {
				IndexType indexType = IndexType.createPrimaryKey(table.getPersistent(), primaryKeyHash);
				String indexName = table.getSchema().getUniqueIndexName(session, table, Constants.PREFIX_PRIMARY_KEY);
				int id = getObjectId(true, false);
				try {
					index = table.addIndex(session, indexName, id, indexColumns, indexType, Index.EMPTY_HEAD, null);
				} finally {
					getSchema().freeUniqueName(indexName);
				}
			}
			index.getIndexType().setBelongsToConstraint(true);
			int constraintId = getObjectId(true, true);
			String name = generateConstraintName(table);
			ConstraintUnique pk = new ConstraintUnique(getSchema(), constraintId, name, table, true);
			pk.setColumns(indexColumns);
			pk.setIndex(index, true);
			constraint = pk;
			break;
		}
		case UNIQUE: {
			IndexColumn.mapColumns(indexColumns, table);
			boolean isOwner = false;
			if (index != null && canUseUniqueIndex(index, table, indexColumns)) {
				isOwner = true;
				index.getIndexType().setBelongsToConstraint(true);
			} else {
				index = getUniqueIndex(table, indexColumns);
				if (index == null) {
					index = createIndex(table, indexColumns, true);
					isOwner = true;
				}
			}
			int id = getObjectId(true, true);
			String name = generateConstraintName(table);
			ConstraintUnique unique = new ConstraintUnique(getSchema(), id, name, table, false);
			unique.setColumns(indexColumns);
			unique.setIndex(index, isOwner);
			constraint = unique;
			break;
		}
		case CHECK: {
			int id = getObjectId(true, true);
			String name = generateConstraintName(table);
			ConstraintCheck check = new ConstraintCheck(getSchema(), id, name, table);
			TableFilter filter = new TableFilter(session, table, null, false, null);
			checkExpression.mapColumns(filter, 0);
			checkExpression = checkExpression.optimize(session);
			check.setExpression(checkExpression);
			check.setTableFilter(filter);
			constraint = check;
			if (checkExisting) {
				check.checkExistingData(session);
			}
			break;
		}
		case REFERENTIAL: {
			Table refTable = refSchema.getTableOrView(session, refTableName);
			session.getUser().checkRight(refTable, Right.ALL);
			boolean isOwner = false;
			IndexColumn.mapColumns(indexColumns, table);
			if (index != null && canUseIndex(index, table, indexColumns)) {
				isOwner = true;
				index.getIndexType().setBelongsToConstraint(true);
			} else {
				index = getIndex(table, indexColumns);
				if (index == null) {
					index = createIndex(table, indexColumns, false);
					isOwner = true;
				}
			}
			if (refIndexColumns == null) {
				Index refIdx = refTable.getPrimaryKey();
				refIndexColumns = refIdx.getIndexColumns();
			} else {
				IndexColumn.mapColumns(refIndexColumns, refTable);
			}
			if (refIndexColumns.length != indexColumns.length) {
				throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
			}
			boolean isRefOwner = false;
			if (refIndex != null && refIndex.getTable() == refTable) {
				isRefOwner = true;
				refIndex.getIndexType().setBelongsToConstraint(true);
			} else {
				refIndex = null;
			}
			if (refIndex == null) {
				refIndex = getUniqueIndex(refTable, refIndexColumns);
				if (refIndex == null) {
					refIndex = createIndex(refTable, refIndexColumns, true);
					isRefOwner = true;
				}
			}
			int id = getObjectId(true, true);
			String name = generateConstraintName(table);
			ConstraintReferential ref = new ConstraintReferential(getSchema(), id, name, table);
			ref.setColumns(indexColumns);
			ref.setIndex(index, isOwner);
			ref.setRefTable(refTable);
			ref.setRefColumns(refIndexColumns);
			ref.setRefIndex(refIndex, isRefOwner);
			if (checkExisting) {
				ref.checkExistingData(session);
			}
			constraint = ref;
			refTable.addConstraint(constraint);
			ref.setDeleteAction(deleteAction);
			ref.setUpdateAction(updateAction);
			break;
		}
		default:
			throw Message.throwInternalError("type=" + type);
		}
		// parent relationship is already set with addConstraint
		constraint.setComment(comment);
		if (table.getTemporary() && !table.getGlobalTemporary()) {
			session.addLocalTempTableConstraint(constraint);
		} else {
			db.addSchemaObject(session, constraint);
		}
		table.addConstraint(constraint);
		return 0;
	}

	private Index createIndex(Table t, IndexColumn[] cols, boolean unique) throws SQLException {
		int indexId = getObjectId(true, false);
		IndexType indexType;
		if (unique) {
			// TODO default index (hash or not; memory or not or same as table)
			// for unique constraints
			indexType = IndexType.createUnique(t.getPersistent(), false);
		} else {
			// TODO default index (memory or not or same as table) for unique
			// constraints
			indexType = IndexType.createNonUnique(t.getPersistent());
		}
		indexType.setBelongsToConstraint(true);
		String prefix = constraintName == null ? "CONSTRAINT" : constraintName;
		String indexName = t.getSchema().getUniqueIndexName(session, t, prefix + "_INDEX_");
		try {
			return t.addIndex(session, indexName, indexId, cols, indexType, Index.EMPTY_HEAD, null);
		} finally {
			getSchema().freeUniqueName(indexName);
		}
	}

	public void setDeleteAction(int action) {
		this.deleteAction = action;
	}

	public void setUpdateAction(int action) {
		this.updateAction = action;
	}

	private Index getUniqueIndex(Table t, IndexColumn[] cols) {
		ObjectArray list = t.getIndexes();
		for (int i = 0; i < list.size(); i++) {
			Index idx = (Index) list.get(i);
			if (canUseUniqueIndex(idx, t, cols)) {
				return idx;
			}
		}
		return null;
	}

	private Index getIndex(Table t, IndexColumn[] cols) {
		ObjectArray list = t.getIndexes();
		for (int i = 0; i < list.size(); i++) {
			Index idx = (Index) list.get(i);
			if (canUseIndex(idx, t, cols)) {
				return idx;
			}
		}
		return null;
	}

	private boolean canUseUniqueIndex(Index idx, Table table, IndexColumn[] cols) {
		if (idx.getTable() != table || !idx.getIndexType().getUnique()) {
			return false;
		}
		Column[] indexCols = idx.getColumns();
		if (indexCols.length > cols.length) {
			return false;
		}
		HashSet set = new HashSet();
		for (int i = 0; i < cols.length; i++) {
			set.add(cols[i].column);
		}
		for (int j = 0; j < indexCols.length; j++) {
			// all columns of the index must be part of the list,
			// but not all columns of the list need to be part of the index
			if (!set.contains(indexCols[j])) {
				return false;
			}
		}
		return true;
	}

	private boolean canUseIndex(Index index, Table table, IndexColumn[] cols) {
		if (index.getTable() != table || index.getCreateSQL() == null) {
			// can't use the scan index or index of another table
			return false;
		}
		Column[] indexCols = index.getColumns();
		if (indexCols.length < cols.length) {
			return false;
		}
		for (int j = 0; j < cols.length; j++) {
			// all columns of the list must be part of the index,
			// but not all columns of the index need to be part of the list
			// holes are not allowed (index=a,b,c & list=a,b is ok; but list=a,c
			// is not)
			int idx = index.getColumnIndex(cols[j].column);
			if (idx < 0 || idx >= cols.length) {
				return false;
			}
		}
		return true;
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

			queryProxy = queryProxyManager.getQueryProxy(getSchema().getTableOrView(session, tableName).getFullName());

			if (queryProxy == null){
				queryProxy = QueryProxy.getQueryProxyAndLock(getSchema().getTableOrView(session, tableName), LockType.WRITE, session.getDatabase());
			}

			return queryProxy;
		}

		return QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstanceInWrapper());

	}

	/**
	 * True if the table involved in the prepared statement is a regular table - i.e. not an H2O meta-data table.
	 */
	protected boolean isRegularTable() {
		boolean isLocal = session.getDatabase().isTableLocal(getSchema());
		return Constants.IS_H2O && !session.getDatabase().isManagementDB() && !isStartup() && !internalQuery && !isLocal;

	}

	public void setConstraintName(String constraintName) {
		this.constraintName = constraintName;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getType() {
		return type;
	}

	public void setCheckExpression(Expression expression) {
		this.checkExpression = expression;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void setIndexColumns(IndexColumn[] indexColumns) {
		this.indexColumns = indexColumns;
	}

	public IndexColumn[] getIndexColumns() {
		return indexColumns;
	}

	/**
	 * Set the referenced table.
	 *
	 * @param refSchema the schema
	 * @param ref the table name
	 */
	public void setRefTableName(Schema refSchema, String ref) {
		this.refSchema = refSchema;
		this.refTableName = ref;
	}

	public void setRefIndexColumns(IndexColumn[] indexColumns) {
		this.refIndexColumns = indexColumns;
	}

	public void setIndex(Index index) {
		this.index = index;
	}

	public void setRefIndex(Index refIndex) {
		this.refIndex = refIndex;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public void setCheckExisting(boolean b) {
		this.checkExisting = b;
	}

	public void setPrimaryKeyHash(boolean b) {
		this.primaryKeyHash = b;
	}

}
