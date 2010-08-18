/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement UPDATE
 */
public class Update extends Prepared {

	private Expression condition;
	private TableFilter tableFilter;
	private Expression[] expressions;
	private QueryProxy queryProxy = null;

	public Update(Session session, boolean internalQuery) {
		super(session, internalQuery);
	}

	public void setTableFilter(TableFilter tableFilter) {
		this.tableFilter = tableFilter;
		Table table = tableFilter.getTable();
		expressions = new Expression[table.getColumns().length];
	}

	public void setCondition(Expression condition) {
		this.condition = condition;
	}

	/**
	 * Add an assignment of the form column = expression.
	 * 
	 * @param column
	 *            the column
	 * @param expression
	 *            the expression
	 */
	public void setAssignment(Column column, Expression expression)
			throws SQLException {
		int id = column.getColumnId();
		if (expressions[id] != null) {
			throw Message.getSQLException(ErrorCode.DUPLICATE_COLUMN_NAME_1,
					column.getName());
		}
		expressions[id] = expression;
		if (expression instanceof Parameter) {
			Parameter p = (Parameter) expression;
			p.setColumn(column);
		}
	}

	public int update(String transactionName) throws SQLException {
		tableFilter.startQuery(session);
		tableFilter.reset();
		RowList rows = new RowList(session);
		try {
			Table table = tableFilter.getTable();
			setTable(table);
			session.getUser().checkRight(table, Right.UPDATE);

			/*
			 * (QUERY PROPAGATED TO ALL REPLICAS).
			 */
			if (isRegularTable()
					&& ((queryProxy != null && queryProxy.getNumberOfReplicas() > 1) || !isReplicaLocal(queryProxy))) {

				String sql = null;
				if (isPreparedStatement()) {
					sql = adjustForPreparedStatement();
				} else {
					sql = sqlStatement;
				}

				if (queryProxy == null)
					queryProxy = new QueryProxy(session.getDatabase()
							.getLocalDatabaseInstanceInWrapper()); // in case of
																	// MERGE
																	// statement.
				return queryProxy.executeUpdate(sql, transactionName, session);
			}

			table.fireBefore(session);
			table.lock(session, true, false);
			int columnCount = table.getColumns().length;
			// get the old rows, compute the new rows
			setCurrentRowNumber(0);
			int count = 0;
			while (tableFilter.next()) {
				checkCanceled();
				setCurrentRowNumber(count + 1);
				if (condition == null
						|| Boolean.TRUE.equals(condition
								.getBooleanValue(session))) {
					Row oldRow = tableFilter.get();
					Row newRow = table.getTemplateRow();
					for (int i = 0; i < columnCount; i++) {
						Expression newExpr = expressions[i];
						Value newValue;
						if (newExpr == null) {
							newValue = oldRow.getValue(i);
						} else if (newExpr == ValueExpression.getDefault()) {
							Column column = table.getColumn(i);
							Expression defaultExpr = column
									.getDefaultExpression();
							Value v;
							if (defaultExpr == null) {
								v = column.validateConvertUpdateSequence(
										session, null);
							} else {
								v = defaultExpr.getValue(session);
							}
							int type = column.getType();
							newValue = v.convertTo(type);
						} else {
							Column column = table.getColumn(i);
							newValue = newExpr.getValue(session).convertTo(
									column.getType());
						}
						newRow.setValue(i, newValue);
					}
					table.validateConvertUpdateSequence(session, newRow);
					if (table.fireRow()) {
						table.fireBeforeRow(session, oldRow, newRow);
					}
					rows.add(oldRow);
					rows.add(newRow);
					count++;
				}
			}
			// TODO self referencing referential integrity constraints
			// don't work if update is multi-row and 'inversed' the condition!
			// probably need multi-row triggers with 'deleted' and 'inserted'
			// at the same time. anyway good for sql compatibility
			// TODO update in-place (but if the position changes,
			// we need to update all indexes) before row triggers

			// the cached row is already updated - we need the old values
			table.updateRows(this, session, rows);
			if (table.fireRow()) {
				rows.invalidateCache();
				for (rows.reset(); rows.hasNext();) {
					checkCanceled();
					Row o = rows.next();
					Row n = rows.next();
					table.fireAfterRow(session, o, n);
				}
			}
			table.fireAfter(session);
			return count;
		} finally {
			rows.close();
		}
	}

	/**
	 * Adjusts the sqlStatement string to be a valid prepared statement. This is
	 * used when propagating prepared statements within the system.
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	private String adjustForPreparedStatement() throws SQLException {

		String sql = null;
		try {

			// Adjust the sqlStatement string to contain actual data.

			// if this is a prepared statement the SQL must look like: update
			// bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
			// use the loop structure from below to obtain this information. how
			// do you know if it is a prepared statement.

			Expression[] expr = expressions;
			String[] values = new String[expressions.length + 1]; // The last
																	// expression
																	// is the
																	// where
																	// condition,
																	// everything
																	// before it
																	// is part
																	// of the
																	// set
																	// expression.
			Column[] columns = table.getColumns();

			/*
			 * 'Expressions' stores all of the expressions being set by this
			 * update. The expression will be null if nothing is being set.
			 * 'Condition' stores the set condition.
			 */
			for (int i = 0; i < (expressions.length); i++) {

				Column c = columns[i];
				int index = c.getColumnId();
				Expression e = expr[i];

				evaluateExpression(e, values, i, c.getType(), expr);
			}

			int y = 2;

			Comparison comparison = ((Comparison) condition);
			Expression setExpression = comparison.getExpression(false);

			evaluateExpression(setExpression, values, y,
					setExpression.getType(), expr);

			// Edit the SQL String
			// Example: update bahrain set Name=? where ID=? {1: 'PILOT_1', 2:
			// 1};
			sql = new String(sqlStatement) + " {";

			boolean addComma = false;
			int count = 1;
			for (int i = 1; i <= values.length; i++) {
				if (values[i - 1] != null) {
					if (addComma)
						sql += ", ";
					else
						addComma = true;

					sql += count + ": " + values[i - 1];

					count++;
				}
			}
			sql += "};";
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return sql;
	}

	private void evaluateExpression(Expression e, String[] values, int i,
			int colummType, Expression[] expr) throws SQLException {
		// Only add the expression if it is unspecified in the query (there will
		// be an instance of parameter somewhere).
		if (e != null && e instanceof Parameter
				|| ((e instanceof Operation) && e.toString().contains("?"))) {
			// e can be null (DEFAULT)
			e = e.optimize(session);
			try {
				Value v = e.getValue(session).convertTo(colummType);
				values[i] = v.toString();
				// newRow.setValue(index, v);
			} catch (SQLException ex) {
				throw setRow(ex, 0, getSQL(expr));
			}
		}
	}

	public String getPlanSQL() {
		StringBuilder buff = new StringBuilder();
		buff.append("UPDATE ");
		buff.append(tableFilter.getPlanSQL(false));
		buff.append("\nSET ");
		Table table = tableFilter.getTable();
		int columnCount = table.getColumns().length;
		for (int i = 0, j = 0; i < columnCount; i++) {
			Expression newExpr = expressions[i];
			if (newExpr != null) {
				if (j > 0) {
					buff.append(",\n");
				}
				j++;
				Column column = table.getColumn(i);
				buff.append(column.getName());
				buff.append(" = ");
				buff.append(newExpr.getSQL());
			}
		}
		if (condition != null) {
			buff.append("\nWHERE " + StringUtils.unEnclose(condition.getSQL()));
		}
		return buff.toString();
	}

	public void prepare() throws SQLException {
		if (condition != null) {
			condition.mapColumns(tableFilter, 0);
			condition = condition.optimize(session);
			condition.createIndexConditions(session, tableFilter);
		}
		for (int i = 0; i < expressions.length; i++) {
			Expression expr = expressions[i];
			if (expr != null) {
				expr.mapColumns(tableFilter, 0);
				expressions[i] = expr.optimize(session);
			}
		}
		PlanItem item = tableFilter.getBestPlanItem(session);
		tableFilter.setPlanItem(item);
		tableFilter.prepare();
	}

	public boolean isTransactional() {
		return true;
	}

	public LocalResult queryMeta() {
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#acquireLocks()
	 */
	@Override
	public void acquireLocks(QueryProxyManager queryProxyManager)
			throws SQLException {
		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()) {

			queryProxy = queryProxyManager.getQueryProxy(tableFilter.getTable()
					.getFullName());

			if (queryProxy == null) {
				queryProxy = QueryProxy.getQueryProxyAndLock(
						tableFilter.getTable(), LockType.WRITE,
						session.getDatabase());
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
		boolean isLocal = session.getDatabase().isTableLocal(
				tableFilter.getTable().getSchema());
		return Constants.IS_H2O && !session.getDatabase().isManagementDB()
				&& !internalQuery && !isLocal;
	}

}
