/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Set;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.table.Table;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockType;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * A prepared statement.
 */
public abstract class Prepared {

	/**
	 * The session.
	 */
	protected Session session;

	/**
	 * The SQL string.
	 */
	protected String sqlStatement;

	/**
	 * The position of the head record (used for indexes).
	 */
	protected int headPos = Index.EMPTY_HEAD;

	/**
	 * The list of parameters.
	 */
	protected ObjectArray parameters;

	/**
	 * H2O. Whether this query is being executed on startup as a meta-record. If
	 * it is, H2O needs to perform fewer checks with the remote System Table.
	 */
	private boolean startup = false;

	/**
	 * If the query should be prepared before each execution. This is set for
	 * queries with LIKE ?, because the query plan depends on the parameter
	 * value.
	 */
	protected boolean prepareAlways;

	private long modificationMetaId;
	private Command command;
	private int objectId;
	private int currentRowNumber;

	protected Table table;

	protected boolean internalQuery;

	/**
	 * True if every replica is local. This will only be the case if there is
	 * only one replica.
	 * 
	 * @return
	 */
	protected boolean isReplicaLocal(QueryProxy queryProxy) {

		for (DatabaseInstanceWrapper replica : queryProxy.getReplicaLocations()) {
			if (!this.session.getDatabase().getURL().equals(replica.getURL()))
				return false;
		}

		return true;
	}

	/**
	 * Create a new object.
	 * 
	 * @param session
	 *            the session
	 * @param internalQuery2
	 */
	public Prepared(Session session, boolean internalQuery) {
		this.session = session;
		this.internalQuery = internalQuery;
		modificationMetaId = session.getDatabase().getModificationMetaId();
	}

	/**
	 * Check if this command is transactional. If it is not, then it forces the
	 * current transaction to commit.
	 * 
	 * @return true if it is
	 */
	public abstract boolean isTransactional();

	/**
	 * Get an empty result set containing the meta data.
	 * 
	 * @return an empty result set
	 */
	public abstract LocalResult queryMeta() throws SQLException;

	/**
	 * Check if this command is read only.
	 * 
	 * @return true if it is
	 */
	public boolean isReadOnly() {
		return false;
	}

	/**
	 * Check if the statement needs to be re-compiled.
	 * 
	 * @return true if it must
	 */
	public boolean needRecompile() throws SQLException {
		Database db = session.getDatabase();
		if (db == null) {
			throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN);
		}
		// TODO parser: currently, compiling every create/drop/... twice!
		// because needRecompile return true even for the first execution
		return SysProperties.RECOMPILE_ALWAYS || prepareAlways
				|| modificationMetaId < db.getModificationMetaId();
	}

	/**
	 * Get the meta data modification id of the database when this statement was
	 * compiled.
	 * 
	 * @return the meta data modification id
	 */
	long getModificationMetaId() {
		return modificationMetaId;
	}

	/**
	 * Set the meta data modification id of this statement.
	 * 
	 * @param id
	 *            the new id
	 */
	void setModificationMetaId(long id) {
		this.modificationMetaId = id;
	}

	/**
	 * Set the parameter list of this statement.
	 * 
	 * @param parameters
	 *            the parameter list
	 */
	public void setParameterList(ObjectArray parameters) {
		this.parameters = parameters;
	}

	/**
	 * Get the parameter list.
	 * 
	 * @return the parameter list
	 */
	public ObjectArray getParameters() {
		return parameters;
	}

	/**
	 * Check if all parameters have been set.
	 * 
	 * @throws SQLException
	 *             if any parameter has not been set
	 */
	protected void checkParameters() throws SQLException {
		for (int i = 0; parameters != null && i < parameters.size(); i++) {
			Parameter param = (Parameter) parameters.get(i);
			param.checkSet();
		}
	}

	/**
	 * Set the command.
	 * 
	 * @param command
	 *            the new command
	 */
	public void setCommand(Command command) {
		this.command = command;
	}

	/**
	 * Check if this object is a query.
	 * 
	 * @return true if it is
	 */
	public boolean isQuery() {
		return false;
	}

	/**
	 * Prepare this statement.
	 */
	public void prepare() throws SQLException {
		// nothing to do
	}

	/**
	 * Execute the statement.
	 * 
	 * @return the update count
	 * @throws SQLException
	 *             if it is a query
	 * @throws RemoteException
	 */
	public int update() throws SQLException, RemoteException {
		throw Message.getSQLException(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
	}

	/**
	 * H2O. Execute the statement.
	 * 
	 * @param transactionName
	 * @return
	 * @throws SQLException
	 * @throws RemoteException
	 */
	public int update(String transactionName) throws SQLException,
			RemoteException {

		/*
		 * If the subclass doesn't override this method, then it does not
		 * propagate the query to a remote machine. As a result, it does not
		 * need to know the transactionName, so it is acceptable to simply call
		 * update().
		 */
		int result = update();

		/*
		 * Because these subclasses don't propagate the query, they also don't
		 * do anything to prepare the transaction locally. Consquently this
		 * action is done here.
		 */
		prepareTransaction(transactionName);

		return result;
	}

	/**
	 * Prepare the commit of a recently executed update.
	 * 
	 * @param transactionName
	 *            Transaction name to be given to this transaction.
	 * @throws SQLException
	 *             Thrown if the PREPARE COMMIT statement fails.
	 * 
	 */
	protected void prepareTransaction(String transactionName)
			throws SQLException {
		if (!isTransactionCommand()) {
			Command command = new Parser(session, true)
					.prepareCommand("PREPARE COMMIT " + transactionName);
			command.executeUpdate();
		}
	}

	/**
	 * Execute the query.
	 * 
	 * @param maxrows
	 *            the maximum number of rows to return
	 * @return the result set
	 * @throws SQLException
	 *             if it is not a query
	 */
	public LocalResult query(int maxrows) throws SQLException {
		throw Message.getSQLException(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	/**
	 * Set the SQL statement.
	 * 
	 * @param sql
	 *            the SQL statement
	 */
	public void setSQL(String sql) {
		this.sqlStatement = sql;
	}

	/**
	 * Get the SQL statement.
	 * 
	 * @return the SQL statement
	 */
	public String getSQL() {
		return sqlStatement;
	}

	/**
	 * Get the object id to use for the database object that is created in this
	 * statement. This id is only set when the object is persistent. If not set,
	 * this method returns 0.
	 * 
	 * @return the object id or 0 if not set
	 */
	protected int getCurrentObjectId() {
		return objectId;
	}

	/**
	 * Get the current object id, or get a new id from the database. The object
	 * id is used when creating new database object (CREATE statement).
	 * 
	 * @param needFresh
	 *            if a fresh id is required
	 * @param dataFile
	 *            if the object id is used for the
	 * @return the object id
	 */
	protected int getObjectId(boolean needFresh, boolean dataFile) {
		Database db = session.getDatabase();
		int id = objectId;
		if (id == 0) {
			id = db.allocateObjectId(needFresh, dataFile);
		}
		objectId = 0;
		return id;
	}

	/**
	 * Get the SQL statement with the execution plan.
	 * 
	 * @return the execution plan
	 */
	public String getPlanSQL() {
		return null;
	}

	/**
	 * Check if this statement was canceled.
	 * 
	 * @throws SQLException
	 *             if it was canceled
	 */
	public void checkCanceled() throws SQLException {
		session.checkCanceled();
		Command c = command != null ? command : session.getCurrentCommand();
		if (c != null) {
			c.checkCanceled();
		}
	}

	/**
	 * Set the object id for this statement.
	 * 
	 * @param i
	 *            the object id
	 */
	public void setObjectId(int i) {
		this.objectId = i;
	}

	/**
	 * Set the head position.
	 * 
	 * @param headPos
	 *            the head position
	 */
	public void setHeadPos(int headPos) {
		this.headPos = headPos;
	}

	/**
	 * Set the session for this statement.
	 * 
	 * @param currentSession
	 *            the new session
	 */
	public void setSession(Session currentSession) {
		this.session = currentSession;
	}

	/**
	 * Print information about the statement executed if info trace level is
	 * enabled.
	 * 
	 * @param startTime
	 *            when the statement was started
	 * @param count
	 *            the update count
	 */
	void trace(long startTime, int count) throws SQLException {
		if (session.getTrace().isInfoEnabled()) {
			long time = System.currentTimeMillis() - startTime;
			String params;
			if (parameters.size() > 0) {
				StringBuilder buff = new StringBuilder(parameters.size() * 10);
				buff.append(" {");
				for (int i = 0; i < parameters.size(); i++) {
					if (i > 0) {
						buff.append(", ");
					}
					buff.append(i + 1);
					buff.append(": ");
					Expression e = (Expression) parameters.get(i);
					Value v = e.getValue(session);
					buff.append(v.getTraceSQL());
				}
				buff.append("}");
				params = buff.toString();
			} else {
				params = "";
			}
			session.getTrace().infoSQL(sqlStatement, params, count, time);
		}
	}

	/**
	 * Set the prepare always flag. If set, the statement is re-compiled
	 * whenever it is executed.
	 * 
	 * @param prepareAlways
	 *            the new value
	 */
	public void setPrepareAlways(boolean prepareAlways) {
		this.prepareAlways = prepareAlways;
	}

	/**
	 * Set the current row number.
	 * 
	 * @param rowNumber
	 *            the row number
	 */
	protected void setCurrentRowNumber(int rowNumber) {
		this.currentRowNumber = rowNumber;
	}

	/**
	 * Get the current row number.
	 * 
	 * @return the row number
	 */
	public int getCurrentRowNumber() {
		return currentRowNumber;
	}

	/**
	 * Convert the statement to a String.
	 * 
	 * @return the SQL statement
	 */
	public String toString() {
		return sqlStatement;
	}

	/**
	 * Get the SQL snippet of the value list.
	 * 
	 * @param values
	 *            the value list
	 * @return the SQL snippet
	 */
	protected String getSQL(Value[] values) {
		StringBuilder buff = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				buff.append(", ");
			}
			Value v = values[i];
			if (v != null) {
				buff.append(v.getSQL());
			}
		}
		return buff.toString();
	}

	/**
	 * Get the SQL snippet of the expression list.
	 * 
	 * @param list
	 *            the expression list
	 * @return the SQL snippet
	 */
	protected String getSQL(Expression[] list) {
		StringBuilder buff = new StringBuilder();
		for (int i = 0; i < list.length; i++) {
			if (i > 0) {
				buff.append(", ");
			}
			Expression e = list[i];
			if (e != null) {
				buff.append(e.getSQL());
			}
		}
		return buff.toString();
	}

	/**
	 * Set the SQL statement of the exception to the given row.
	 * 
	 * @param ex
	 *            the exception
	 * @param rowId
	 *            the row number
	 * @param values
	 *            the values of the row
	 * @return the exception
	 */
	protected SQLException setRow(SQLException ex, int rowId, String values) {
		if (ex instanceof JdbcSQLException) {
			JdbcSQLException e = (JdbcSQLException) ex;
			StringBuilder buff = new StringBuilder();
			if (sqlStatement != null) {
				buff.append(sqlStatement);
			}
			buff.append(" -- ");
			if (rowId > 0) {
				buff.append("row #").append(rowId + 1).append(" ");
			}
			buff.append("(").append(values).append(")");
			e.setSQL(buff.toString());
		}
		return ex;
	}

	/**
	 * Whether this prepared statement is being created as part of the execution
	 * of a meta-record.
	 * 
	 * @param startup
	 *            True, if it is; otherwise, false.
	 */
	public void setStartup(boolean startup) {
		this.startup = startup;
	}

	/**
	 * Whether this prepared statement is being created as part of the execution
	 * of a meta-record.
	 * 
	 * @param startup
	 */
	public boolean isStartup() {
		return startup;
	}

	/**
	 * True if the table involved in the prepared statement is a regular table -
	 * i.e. not an H2O meta-data table.
	 */
	protected boolean isRegularTable() {
		Set<String> localSchema = session.getDatabase().getLocalSchema();
		try {
			return Constants.IS_H2O && !session.getDatabase().isManagementDB()
					&& !internalQuery
					&& !localSchema.contains(table.getSchema().getName());
		} catch (NullPointerException e) {
			// Shouldn't occur, ever. Something should have probably overridden
			// this if it can't possibly know about a particular table.
			ErrorHandling.hardError("isRegularTable() check failed.");
			return false;
		}
	}

	public void setTable(Table table) {
		this.table = table;
	}

	/**
	 * Request a lock for the given query, in preparation for its execution.
	 * Must be called before update(). This method will be overriden if a
	 * QueryProxy can be returned - prepared statements have to acquire a lock
	 * in this manner.
	 * 
	 * @param queryProxyManager
	 * @return
	 * @throws SQLException
	 */
	public void acquireLocks(QueryProxyManager queryProxyManager)
			throws SQLException {
		QueryProxy.getQueryProxyAndLock(table, LockType.READ,
				session.getDatabase());
	}

	/**
	 * Should this command be propagated to multiple sites. This method will be
	 * overridedn if true.
	 */
	public boolean shouldBePropagated() {
		return false;
	}

	/**
	 * Is this statement an instance of transaction command.
	 * 
	 * @return
	 */
	public boolean isTransactionCommand() {
		return false;
	}

	/**
	 * @param internalQuery
	 *            the internalQuery to set
	 */
	public void setInternalQuery(boolean internalQuery) {
		this.internalQuery = internalQuery;
	}

	/**
	 * @param preparedStatement
	 */
	public void setPreparedStatement(boolean preparedStatement) {
	}

	/**
	 * @return the preparedStatement
	 */
	public boolean isPreparedStatement() {
		return sqlStatement.contains("?");
		// return preparedStatement;
	}

}
