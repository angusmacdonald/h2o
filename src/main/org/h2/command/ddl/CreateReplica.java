/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;

import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.SchemaManager;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.LinkedIndex;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.table.TableLinkConnection;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * This class represents the statement
 * CREATE REPLICA
 */
public class CreateReplica extends SchemaCommand {

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

	private TableLinkConnection conn;
	private SchemaManager schemaManager;

	private boolean storesLowerCase = false;
	private boolean storesMixedCase = false;
	private boolean supportsMixedCaseIdentifiers = false;

	/**
	 * Array containing all of the insert statements required for this replicas state to match that of the primary.
	 */
	private String[] inserts = null;

	public CreateReplica(Session session, Schema schema) {
		super(session, schema);
		schemaManager = SchemaManager.getInstance();
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
		tableName = "Replica" + tableName; //XXX quick hack to check everything else works.

		// TODO rights: what rights are required to create a table?
		session.commit(true);
		Database db = session.getDatabase();
		if (!db.isPersistent()) {
			persistent = false;
		}
		if (getSchema().findTableOrView(session, tableName) != null) {
			if (ifNotExists) {
				return 0;
			}
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
		}

		/*
		 * #########################################################################
		 * 
		 *  H20. Check that the table doesn't already exist elsewhere.
		 * 
		 * #########################################################################
		 */

		Parser parser = null;

		if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_")){

			parser = new Parser(session);
			boolean throwException = false;
			try { //XXX this is a hack. shouldn't rely on exception being thrown.

				parser.findViaSchemaManager(tableName);

				throwException = true; //code shouldn't reach this point.
			} catch (SQLException sqlE){
				//not found, meaning this create table command can be executed.
			}

			if (throwException){
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
			}
		}
		/*
		 * #########################################################################
		 * #########################################################################
		 */       


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
					insert = new Insert(session);
					insert.setQuery(asQuery);
					insert.setTable(table);
					insert.prepare();
					insert.update();
				} finally {
					session.setUndoLogEnabled(old);
				}
			}


			if (Constants.IS_H2O && !db.isManagementDB() && !tableName.startsWith("H2O_")){

				/*
				 * #########################################################################
				 * 
				 *  Copy over data.
				 * 
				 * #########################################################################
				 */
				try {
					if (inserts != null){
						for (String insert: inserts){
							Prepared command = session.prepare(insert);
							command.update();
						}
					}
				} catch (SQLException e){
					e.printStackTrace();
					throw new SQLException("Failed to copy data across to new replica.");
				}

				/*
				 * #########################################################################
				 * 
				 *  Create a schema manager entry.
				 * 
				 * #########################################################################
				 */

				SchemaManager sm = SchemaManager.getInstance(session); //db.getSystemSession()
				sm.addTableInformation(tableName, table.getModificationId(), db.getDatabaseLocation(), table.getTableType(), 
						db.getLocalMachineAddress(), db.getLocalMachinePort(), "tcp");	
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

	/**
	 * Get the primary location of the given table.
	 * @throws JdbcSQLException 
	 */
	public void readSQL() throws JdbcSQLException {
		String tableLocation = "";

		try {
			tableLocation = SchemaManager.getInstance().getPrimaryReplicaLocation(tableName);
		} catch (SQLException e) {
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
		}

		//TODO connect to the given location using hte same method as linked tables, and get the appropriate create table SQL.
		try {
			connect(tableLocation);
		} catch (SQLException e) {
			throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN, tableName);
		}
	}

	private void connect(String tableLocation) throws SQLException {
		Database db = session.getDatabase();

		conn = db.getLinkConnection("org.h2.Driver", tableLocation, SchemaManager.USERNAME, SchemaManager.PASSWORD);
		synchronized (conn) {
			try {
				readMetaData();
				getTableData();
			} catch (SQLException e) {
				conn.close();
				conn = null;
				throw e;
			}
		}
	}

	/**
	 * Get the data required to fill up this replica (to match the contents of the primary).
	 * 
	 * Data is then stored in the 'inserts' field.
	 */
	private void getTableData() {
		// check if the table is accessible
		Statement stat = null;
		try {
			stat = conn.getConnection().createStatement();
			ResultSet rs = stat.executeQuery("SCRIPT TABLE " + tableName);

			String[] inserts = new String[rs.getFetchSize()]; //XXX the fetch size might be less than the number of database entries.
			int i = 0;
			while (rs.next()){
				inserts[i++] = rs.getString(1);
			}

			this.inserts = inserts;

			rs.close();
		} catch (SQLException e) {
			System.err.println("Failed to fill replica.");
			e.printStackTrace();
		} finally {
			JdbcUtils.closeSilently(stat);
		}
	}

	private void readMetaData() throws SQLException {
		String originalSchema = null; //XXX this won't work if the table is in something other than the default schema.

		DatabaseMetaData meta = conn.getConnection().getMetaData();
		storesLowerCase = meta.storesLowerCaseIdentifiers();
		storesMixedCase = meta.storesMixedCaseIdentifiers();
		supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
		ResultSet rs = meta.getTables(null, null, tableName, null);
		if (rs.next() && rs.next()) {
			throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH, tableName);
		}
		rs.close();
		rs = meta.getColumns(null, originalSchema, tableName, null); 
		int i = 0;
		ObjectArray columnList = new ObjectArray();
		HashMap columnMap = new HashMap();
		String catalog = null, schema = null;
		while (rs.next()) {
			String thisCatalog = rs.getString("TABLE_CAT");
			if (catalog == null) {
				catalog = thisCatalog;
			}
			String thisSchema = rs.getString("TABLE_SCHEM");
			if (schema == null) {
				schema = thisSchema;
			}
			if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
				// if the table exists in multiple schemas or tables,
				// use the alternative solution
				columnMap.clear();
				columnList.clear();
				break;
			}
			String n = rs.getString("COLUMN_NAME");
			n = convertColumnName(n);
			int sqlType = rs.getInt("DATA_TYPE");
			long precision = rs.getInt("COLUMN_SIZE");
			precision = convertPrecision(sqlType, precision);
			int scale = rs.getInt("DECIMAL_DIGITS");
			int displaySize = MathUtils.convertLongToInt(precision);
			int type = DataType.convertSQLTypeToValueType(sqlType);
			Column col = new Column(n, type, precision, scale, displaySize);
			addColumn(col);
		}
		rs.close();

		String qualifiedTableName = "";

		if (tableName.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
			qualifiedTableName = schema + "." + tableName;
		} else {
			qualifiedTableName = tableName;
		}

		// check if the table is accessible
		Statement stat = null;
		try {
			stat = conn.getConnection().createStatement();
			rs = stat.executeQuery("SELECT * FROM " + qualifiedTableName + " T WHERE 1=0");
			if (columns.size() == 0) {
				// alternative solution
				ResultSetMetaData rsMeta = rs.getMetaData();
				for (i = 0; i < rsMeta.getColumnCount();) {
					String n = rsMeta.getColumnName(i + 1);
					n = convertColumnName(n);
					int sqlType = rsMeta.getColumnType(i + 1);
					long precision = rsMeta.getPrecision(i + 1);
					precision = convertPrecision(sqlType, precision);
					int scale = rsMeta.getScale(i + 1);
					int displaySize = rsMeta.getColumnDisplaySize(i + 1);
					int type = DataType.convertSQLTypeToValueType(sqlType);
					Column col = new Column(n, type, precision, scale, displaySize);
					addColumn(col);
				}
			}
			rs.close();
		} catch (SQLException e) {
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new String[] { tableName + "("
					+ e.toString() + ")" }, e);
		} finally {
			JdbcUtils.closeSilently(stat);
		}



		try {
			rs = meta.getPrimaryKeys(null, originalSchema, tableName);
		} catch (SQLException e) {
			// Some ODBC bridge drivers don't support it:
			// some combinations of "DataDirect SequeLink(R) for JDBC"
			// http://www.datadirect.com/index.ssp
			rs = null;
		}
		String pkName = "";
		ObjectArray list;
		if (rs != null && rs.next()) {
			// the problem is, the rows are not sorted by KEY_SEQ
			list = new ObjectArray();
			do {
				int idx = rs.getInt("KEY_SEQ");
				if (pkName == null) {
					pkName = rs.getString("PK_NAME");
				}
				while (list.size() < idx) {
					list.add(null);
				}
				String col = rs.getString("COLUMN_NAME");
				col = convertColumnName(col);
				Column column = (Column) columnMap.get(col);
				list.set(idx - 1, column);
			} while (rs.next());
			addIndex(list, IndexType.createPrimaryKey(false, false)); //XXX doesn't do anything currently. Might be necessary later.
			rs.close();
		}
		try {
			rs = meta.getIndexInfo(null, originalSchema, tableName, false, true);
		} catch (SQLException e) {
			// Oracle throws an exception if the table is not found or is a
			// SYNONYM
			rs = null;
		}
		String indexName = null;
		list = new ObjectArray();
		IndexType indexType = null;
		if (rs != null) {
			while (rs.next()) {
				if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
					// ignore index statistics
					continue;
				}
				String newIndex = rs.getString("INDEX_NAME");
				if (pkName.equals(newIndex)) {
					continue;
				}
				if (indexName != null && !indexName.equals(newIndex)) {
					addIndex(list, indexType);
					indexName = null;
				}
				if (indexName == null) {
					indexName = newIndex;
					list.clear();
				}
				boolean unique = !rs.getBoolean("NON_UNIQUE");
				indexType = unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false);
				String col = rs.getString("COLUMN_NAME");
				col = convertColumnName(col);
				Column column = (Column) columnMap.get(col);
				list.add(column);
			}
			rs.close();
		}
		if (indexName != null) {
			addIndex(list, indexType);
		}
	}


	private void addIndex(ObjectArray list, IndexType indexType) {
		//	        Column[] cols = new Column[list.size()];
		//	        list.toArray(cols);
		//	        Index index = new LinkedIndex(this, 0, IndexColumn.wrap(cols), indexType);
		//	        indexes.add(index);
	}

	private String convertColumnName(String columnName) {
		if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
			columnName = StringUtils.toUpperEnglish(columnName);
		} else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
			// TeraData
			columnName = StringUtils.toUpperEnglish(columnName);
		}
		return columnName;
	}

	private long convertPrecision(int sqlType, long precision) {
		// workaround for an Oracle problem
		// the precision reported by Oracle is 7 for a date column
		switch (sqlType) {
		case Types.DATE:
			precision = Math.max(ValueDate.PRECISION, precision);
			break;
		case Types.TIMESTAMP:
			precision = Math.max(ValueTimestamp.PRECISION, precision);
			break;
		case Types.TIME:
			precision = Math.max(ValueTime.PRECISION, precision);
			break;
		}
		return precision;
	}

}
