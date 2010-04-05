/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.h2o.comms.remote.DataManagerRemote;
import org.h2.h2o.manager.ISchemaManager;
import org.h2.h2o.manager.MovedException;
import org.h2.h2o.manager.PersistentSchemaManager;
import org.h2.h2o.manager.SchemaManagerReference;
import org.h2.h2o.util.TableInfo;
import org.h2.index.IndexType;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableLinkConnection;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

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

	private boolean storesLowerCase = false;
	private boolean storesMixedCase = false;
	private boolean supportsMixedCaseIdentifiers = false;

	/**
	 * Array containing all of the insert statements required for this replicas state to match that of the primary.
	 */
	private List<String> inserts = null;

	/**
	 * The intended location of the remote replica.
	 */
	private String whereReplicaWillBeCreated = null;

	/**
	 * The location where the data currently exists and can be copied from.
	 */
	private String whereDataWillBeTakenFrom = null;

	/**
	 * The next table to be replicated if it is to be done with more than one.
	 */
	private CreateReplica next = null;
	private Set<IndexColumn[]> setOfIndexColumns;
	private Set<IndexType> pkIndexType;
	private int tableSet = -1; //the set of tables which this replica will belong to.
	private boolean contactSchemaManager = true;

	public CreateReplica(Session session, Schema schema) {
		super(session, schema);

		setOfIndexColumns = new HashSet<IndexColumn[]>();
		pkIndexType = new HashSet<IndexType>();
	}

	public void setQuery(Query query) {
		this.asQuery = query;
	}

	public void setTemporary(boolean temporary) {
		this.temporary = temporary;
	}

	public void setTableName(String tableName) {

		if (tableName.contains(".")){
			this.tableName = tableName.split("\\.")[1];
		} else {
			this.tableName = tableName;
		}
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

	public int update() throws SQLException, RemoteException {

		Database db = session.getDatabase();


		if (whereReplicaWillBeCreated != null || db.getFullDatabasePath().equals(whereReplicaWillBeCreated)){
			int result = pushCommand(whereReplicaWillBeCreated, "CREATE REPLICA " + tableName + " FROM '" + whereDataWillBeTakenFrom + "'", true); //command will be executed elsewhere

			//Update the schema manager here.

			if (result == 0){
				try {
					ISchemaManager sm = db.getSchemaManager(); //db.getSystemSession()

					Table table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);

					if (tableSet  == -1){
						tableSet = sm.getNewTableSetNumber();
					} else {
						if (next != null){
							next.setTableSet(tableSet);
						}
					}

					TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), tableSet, table.getTableType(), db.getDatabaseURL());

					sm.addReplicaInformation(ti);	
				} catch (MovedException e){
					throw new RemoteException("Schema Manager has moved.");
				}
			}

			return result;

		} else {
			readSQL(); //command will be executed here - get the table meta-data and contents.
		}

		// TODO rights: what rights are required to create a table?
		session.commit(true);

		if (!db.isPersistent()) {
			persistent = false;
		}

		if (getSchema().findLocalTableOrView(session, tableName) != null) { //H2O. Check for local version here.
			if (ifNotExists) {
				return 0;
			}
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
		}  



		String fullTableName = getSchema().getName() + "." + tableName; //getSchema().getName() + "." + 

		if (getSchema().findTableOrView(session, fullTableName, LocationPreference.NO_PREFERENCE) == null) { //H2O. Check for the existence of any version. if a linked table version doesn't exist we must create it.
			String createLinkedTable = "\nCREATE LINKED TABLE IF NOT EXISTS " + fullTableName + "('org.h2.Driver', '" + whereDataWillBeTakenFrom + "', '" + 
			PersistentSchemaManager.USERNAME + "', '" + PersistentSchemaManager.PASSWORD + "', '" + fullTableName + "');";
			Parser queryParser = new Parser(session, true);
			Command sqlQuery = queryParser.prepareCommand(createLinkedTable);
			sqlQuery.update();
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
					if (pkColumns[j].columnName == null) 
						pkColumns[j].columnName = pkColumns[j].column.getName();
					if (c.getName().equals(pkColumns[j].columnName)) {
						c.setNullable(false);
					}

					c.setPrimaryKey(true);
				}
			}
		}
		ObjectArray sequences = new ObjectArray();
		for (int i = 0; i < columns.size(); i++) {
			Column c = (Column) columns.get(i);

			if (fullTableName.startsWith("H2O.H2O") && i == 0){ //XXX nasty h2o-specific auto-increment hack.
				c.setAutoIncrement(true, 1, 1);
			}
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
			 * Add indexes to the new table.
			 */


			//			IndexType[] indexTypes = pkIndexType.toArray(new IndexType[0]);
			//			int y = 0;
			//
			//			for (IndexColumn[] indexColumns: setOfIndexColumns){
			//				String indexName = "";
			//
			//				if (indexTypes[y].getPrimaryKey()){
			//					indexName = getSchema().getUniqueIndexName(session, table, Constants.PREFIX_PRIMARY_KEY);
			//
			//				} else {
			//					indexName = table.getSchema().getUniqueIndexName(session, table, Constants.PREFIX_INDEX);
			//				}
			//				table.addIndex(session, indexName, getObjectId(true, false), indexColumns, indexTypes[y], Index.EMPTY_HEAD, "H2O");
			//				
			//				y++;
			//			}

			/*
			 * Copy over the data that we have stored in the 'inserts' set. This section of code loops
			 * through that set and does some fairly primitive string splitting to get each value - there
			 * is an assumption that a comma will never appear in the database TODO fix this! 
			 */

			if (inserts.size() > 1){ //the first entry contains type info //XXX hack.
				Insert command = new Insert(session, true);

				command.setTable(table);

				Column[] columnArray = new Column[columns.size()];
				columns.toArray(columnArray);
				command.setColumns(columnArray);

				ArrayList<Integer> types = new ArrayList<Integer>();

				boolean firstRun = true;
				for (String statement: inserts){
					ObjectArray values = new ObjectArray();
					statement = statement.substring(0, statement.length()-1);


					int i = 0;
					for (String part: statement.split(",")){
						part = part.trim();
						if (firstRun){
							types.add(new Integer(part));
						} else {

							if (part.startsWith("'") && part.endsWith("'")){
								part = part.substring(1, part.length()-1);
							}
							ValueString val = ValueString.get(part);


							values.add(ValueExpression.get(val.convertTo(types.get(i++))));
						}

					}

					if (firstRun){
						firstRun = false;
					} else {
						Expression[] expr = new Expression[values.size()];
						values.toArray(expr);
						command.addRow(expr);
					}

				}

				command.update();
			}

			//	#############################
			//  Add to data manager.
			//	#############################

			if (this.contactSchemaManager){
				try{

					ISchemaManager sm = db.getSchemaManager(); //db.getSystemSession()


					if (tableSet  == -1){
						tableSet = sm.getNewTableSetNumber();
					} else {
						if (next != null){
							next.setTableSet(tableSet);
						}
					}

					TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), tableSet, table.getTableType(), db.getDatabaseURL());

					sm.addReplicaInformation(ti);	

				} catch (MovedException e){
					throw new RemoteException("Schema Manager has moved.");
				}
			}

			if (!tableName.startsWith("H2O_")){
				DataManagerRemote dm = db.getDataManager(getSchema().getName() + "." + tableName);
				try {
					if (dm == null){
						throw new SQLException("Error creating replica for " + tableName + ". Data manager not found.");
					} else {
						dm.addReplicaInformation(table.getModificationId(), db.getDatabaseLocation(), table.getTableType(), 
								db.getLocalMachineAddress(), db.getLocalMachinePort(), db.getConnectionType(), tableSet, db.getSchemaManagerReference().isSchemaManagerLocal());
					} 
				} catch (RemoteException e) {
					ErrorHandling.exceptionError(e, "Error informing data manager of update.");
				} catch (MovedException e) {
					throw new SQLException("Data Manager has moved and can't be accessed.");
				}
			}

		} catch (SQLException e) {
			db.checkPowerOff();
			db.removeSchemaObject(session, table);
			throw e;
		}

		if (next != null) {
			next.update();
		}

		return 0;
	}

	/**
	 * Set the tableSet number for this table.
	 * @param tableSet2
	 */
	private void setTableSet(int tableSet) {
		this.tableSet = tableSet;
	}

	/**
	 * Push a command to a remote machine where it will be properly executed.
	 * @param createReplica true, if the command being pushed is a create replica command. This results in any subsequent tables
	 * involved in the command also being pushed.
	 * @return The result of the update.
	 * @throws SQLException 
	 * @throws RemoteException 
	 */
	private int pushCommand(String remoteDBLocation, String query, boolean createReplica) throws SQLException, RemoteException {

		try{
			Database db = session.getDatabase();

			conn = db.getLinkConnection("org.h2.Driver", remoteDBLocation, PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);

			int result = -1;

			synchronized (conn) {
				try {
					Statement stat = conn.getConnection().createStatement();
					String databaseName = null;


					stat.execute(query);
					result = stat.getUpdateCount();

				} catch (SQLException e) {
					conn.close();
					conn = null;
					e.printStackTrace();
					throw e;
				}
			}

			if (next != null && createReplica) {
				next.update();
			}

			return result;

		} catch (Exception e){
			e.printStackTrace();
			return 0;
		}
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
				String columnName = (columns[i].columnName == null)? columns[i].column.getName() : columns[i].columnName;
				if (!columnName.equals(pkColumns[i].columnName)) {
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
	 * Get the primary location of the given table and get meta-data from that location along with the contents of the table.
	 * @throws JdbcSQLException 
	 */
	public void readSQL() throws JdbcSQLException {

		try {
			connect(whereDataWillBeTakenFrom);
		} catch (SQLException e) {
			e.printStackTrace();
			throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN, tableName);
		}
	}

	private void connect(String tableLocation) throws SQLException {
		Database db = session.getDatabase();

		conn = db.getLinkConnection("org.h2.Driver", tableLocation, PersistentSchemaManager.USERNAME, PersistentSchemaManager.PASSWORD);
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

			//String fullTableName = getSchema().getName() + "." + tableName;

			ResultSet rs = stat.executeQuery("SCRIPT TABLE " + getSchema().getName() + "." + tableName);

			List<String> inserts = new LinkedList<String>();

			while (rs.next()){
				inserts.add(rs.getString(1));
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
		String originalSchema = getSchema().getName();

		DatabaseMetaData meta = conn.getConnection().getMetaData();
		storesLowerCase = meta.storesLowerCaseIdentifiers();
		storesMixedCase = meta.storesMixedCaseIdentifiers();
		supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
		ResultSet rs = meta.getTables(null, originalSchema, tableName, null);
		//		if (rs.next() && rs.next()) { //XXX this is ommited because there are duplicate table entries. does this matter.
		//			throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH, tableName);
		//		}
		rs.close();
		rs = meta.getColumns(null, originalSchema, tableName, null); 

		int i = 0;
		ObjectArray columnList = new ObjectArray();
		HashMap columnMap = new HashMap();
		String catalog = null, schema = null;

		Set<String> currentColumns = new HashSet<String>();
		/*
		 * Iterate over column meta-data.
		 */
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
				//				columnMap.clear();				//XXX this doesn't work in H2O.
				//				columnList.clear();
				break;
			}
			String columnName = rs.getString("COLUMN_NAME");
			
			if (currentColumns.contains(columnName)) //stops duplicate primary keys - this happens with multiple replicas.
				continue;
			
			currentColumns.add(columnName);
			
			columnName = convertColumnName(columnName);
			int sqlType = rs.getInt("DATA_TYPE");
			long precision = rs.getInt("COLUMN_SIZE");
			precision = convertPrecision(sqlType, precision);
			int scale = rs.getInt("DECIMAL_DIGITS");
			int nullable = rs.getInt("NULLABLE");
			int displaySize = MathUtils.convertLongToInt(precision);
			int type = DataType.convertSQLTypeToValueType(sqlType);
			Column col = new Column(columnName, type, precision, scale, displaySize);
			
			col.setNullable(nullable == 1);

			/*
			 * Add this new column to the 'columns' field.
			 */
			addColumn(col);


			columnList.add(col);
			columnMap.put(columnName, col);
		}
		rs.close();

		String qualifiedTableName = "";

		if (tableName.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
			qualifiedTableName = schema + "." + tableName;
		} else {
			qualifiedTableName = tableName;
		}

		/*
		 * Try to access the table, to ensure it actually exists and can be queried.
		 * If no columns were added above, get them from the meta-data which results from this query.
		 */
		Statement stat = null;
		try {
			stat = conn.getConnection().createStatement();
			rs = stat.executeQuery("SELECT * FROM " + qualifiedTableName + " T WHERE 1=0");
			if (columns.size() == 0) {
				// alternative solution
				ResultSetMetaData rsMeta = rs.getMetaData();
				for (i = 0; i < rsMeta.getColumnCount(); i++) {
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
					columnList.add(col);
					columnMap.put(n, col);
				}
			}
			rs.close();
		} catch (SQLException e) {
			throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new String[] { tableName + "("
					+ e.toString() + ")" }, e);
		} finally {
			JdbcUtils.closeSilently(stat);
		}


		/*
		 * Get information on the primary keys for this table.
		 */
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

			/*
			 * Loop through each of the primary keys.
			 */
			do {
				int idx = rs.getInt("KEY_SEQ");
				if (pkName == null) {
					pkName = rs.getString("PK_NAME");
				}

				/*
				 * Loop through adding a new 'null' entry in the object array for each column that may be included later. 
				 */
				while (list.size() < idx) {
					list.add(null);
				}
				String columnName = rs.getString("COLUMN_NAME");
				columnName = convertColumnName(columnName);
				Column column = (Column) columnMap.get(columnName);
				list.set(idx - 1, column);
			} while (rs.next());

			/*
			 * Add this set of primary key columns to an index.
			 */
			addConstraint(list, IndexType.createPrimaryKey(false, false));
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
			/*
			 * Loop through all the indexes on this table
			 */
			while (rs.next()) {
				if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
					// ignore index statistics
					continue;
				}
				String newIndexName = rs.getString("INDEX_NAME");
				if (pkName.equals(newIndexName)) {
					continue;
				}
				if (indexName != null && !indexName.equals(newIndexName)) {
					addConstraint(list, indexType);
					indexName = null;
				}
				if (indexName == null) {
					indexName = newIndexName;
					list.clear();
				}
				boolean unique = !rs.getBoolean("NON_UNIQUE");
				indexType = unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false);
				String columnName = rs.getString("COLUMN_NAME");
				columnName = convertColumnName(columnName);
				Column column = (Column) columnMap.get(columnName);
				list.add(column);
				columnList.add(columnName);
			}
			rs.close();
		}
		if (indexName != null) {
			addConstraint(list, indexType);
		}
	}


	private void addConstraint(ObjectArray list, IndexType indexType) throws SQLException {
		/*
		 * If this is a primary key constraint, do primary key stuff.
		 */
		if (indexType.getPrimaryKey()){
			Column[] cols = new Column[list.size()];
			list.toArray(cols);

			/*
			 * Set all primary key columns to be not nullable.
			 */
			for (Column c: cols){
				if (c != null){
					c.setNullable(false);
				} else {
					ErrorHandling.errorNoEvent("This column was null.");
				}
			}

			IndexColumn[] indexColumn = new IndexColumn[list.size()];
			indexColumn = IndexColumn.wrap(cols);

			AlterTableAddConstraint pk = new AlterTableAddConstraint(session, getSchema(), false, internalQuery);
			pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
			pk.setTableName(tableName);
			pk.setIndexColumns(indexColumn);

			addConstraintCommand(pk);
		}
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

	/**
	 * Sets the location at which the replica will be located. If this method is called that location
	 * is not the local machine, and so the command will be sent remotely to be executed.
	 * @param replicationLocation	The location of the remote database.
	 */
	public void setReplicationLocation(String replicationLocation) {
		this.whereReplicaWillBeCreated = replicationLocation;

		if (whereReplicaWillBeCreated != null && whereReplicaWillBeCreated.startsWith("'") && whereReplicaWillBeCreated.endsWith("'")){
			whereReplicaWillBeCreated = whereReplicaWillBeCreated.substring(1, whereReplicaWillBeCreated.length()-1);
		}

		if (next != null){
			next.setReplicationLocation(whereReplicaWillBeCreated);
		}
	}

	/**
	 * Sets the location at which the primary copy is located. If this method is called that location
	 * is not the local machine - this location is used to get the meta-data and data from the given table.
	 * @param originalLocation
	 * @throws JdbcSQLException 
	 * @throws RemoteException 
	 */
	public void setOriginalLocation(String originalLocation, boolean contactSM) throws SQLException, RemoteException {
		contactSchemaManagerOnCompletion(contactSM);

		this.whereDataWillBeTakenFrom = originalLocation;

		if (whereDataWillBeTakenFrom != null && whereDataWillBeTakenFrom.startsWith("'") && whereDataWillBeTakenFrom.endsWith("'")){
			whereDataWillBeTakenFrom = whereDataWillBeTakenFrom.substring(1, whereDataWillBeTakenFrom.length()-1);
		}

		if (whereDataWillBeTakenFrom == null){

			SchemaManagerReference sm = session.getDatabase().getSchemaManagerReference();

			DataManagerRemote dm;
//			try {
				dm = sm.lookup(new TableInfo(tableName, getSchema().getName()));
//			} catch (MovedException e){
//				throw new RemoteException("Schema Manager has moved.");
//			}

			if (dm == null){
				throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new TableInfo(tableName, getSchema().getName()).toString());
			} else {
				try {
					whereDataWillBeTakenFrom = dm.getLocation();
				} catch (MovedException e) {
					e.printStackTrace();
					System.err.println("FIND NEW DATA MANAGER LOCATION AT THIS POINT.");//TODO find
				}
			}
		}

		if (next != null){
			next.setOriginalLocation(whereDataWillBeTakenFrom, contactSM);
		}
	}

	/**
	 * @param next
	 */
	public void addNextCreateReplica(CreateReplica create) {
		if (next == null) {
			next = create;
		} else {
			next.addNextCreateReplica(create);
		}
	}


	@Override
	public String toString(){
		return tableName;
	}

	/**
	 * @param b
	 */
	public void contactSchemaManagerOnCompletion(boolean b) {
		this.contactSchemaManager = b;
	}

}
