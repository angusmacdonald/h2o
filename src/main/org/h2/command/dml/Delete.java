/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Comparison;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.log.UndoLogRecord;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2o.db.query.QueryProxy;
import org.h2o.db.query.QueryProxyManager;
import org.h2o.db.query.locking.LockType;

/**
 * This class represents the statement DELETE
 */
public class Delete extends Prepared {

	private Expression condition;
	private TableFilter tableFilter;
	private QueryProxy queryProxy = null;

	public Delete(Session session, boolean internalQuery) {
		super(session, internalQuery);
	}

	public void setTableFilter(TableFilter tableFilter) {
		this.tableFilter = tableFilter;
		this.table = tableFilter.getTable();
	}

	public void setCondition(Expression condition) {
		this.condition = condition;
	}

	public int update() throws SQLException {
		return update(null);
	}

	public int update(String transactionName) throws SQLException {
		tableFilter.startQuery(session);
		tableFilter.reset();
		Table table = tableFilter.getTable();
		setTable(table);
		session.getUser().checkRight(table, Right.DELETE);

		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()
				&& (queryProxy.getNumberOfReplicas() > 1 || !isReplicaLocal(queryProxy))) {
			if (queryProxy == null) {
				throw new SQLException("Internal Error: Query Proxy was null.");
			}

			String sql = null;
			if (isPreparedStatement()) {
				sql = adjustForPreparedStatement();
			} else {
				sql = sqlStatement;
			}

			return queryProxy.executeUpdate(sql, transactionName, session);
		}

		table.fireBefore(session);
		table.lock(session, true, false);
		RowList rows = new RowList(session);
		try {
			setCurrentRowNumber(0);
			while (tableFilter.next()) {
				checkCanceled();
				setCurrentRowNumber(rows.size() + 1);
				if (condition == null
						|| Boolean.TRUE.equals(condition
								.getBooleanValue(session))) {
					Row row = tableFilter.get();
					if (table.fireRow()) {
						table.fireBeforeRow(session, row, null);
					}
					rows.add(row);
				}
			}
			for (rows.reset(); rows.hasNext();) {
				checkCanceled();
				Row row = rows.next();
				table.removeRow(session, row);
				session.log(table, UndoLogRecord.DELETE, row);
			}
			if (table.fireRow()) {
				for (rows.reset(); rows.hasNext();) {
					Row row = rows.next();
					table.fireAfterRow(session, row, null);
				}
			}
			table.fireAfter(session);
			return rows.size();
		} finally {
			rows.close();
		}
	}

	private String adjustForPreparedStatement() throws SQLException {
		String[] values = new String[1];

		int y = 0;

		Comparison comparison = ((Comparison) condition);
		Expression setExpression = comparison.getExpression(false);

		evaluateExpression(setExpression, values, y, setExpression.getType());

		// Edit the SQL String
		// Example: update bahrain set Name=? where ID=? {1: 'PILOT_1', 2: 1};
		String sql = new String(sqlStatement) + " {";

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

		return sql;
	}

	private void evaluateExpression(Expression e, String[] values, int i,
			int colummType) throws SQLException {
		// Only add the expression if it is unspecified in the query (there will
		// be an instance of parameter somewhere).
		if (e != null && e instanceof Parameter) {
			// e can be null (DEFAULT)
			e = e.optimize(session);

			Value v = e.getValue(session).convertTo(colummType);
			values[i] = v.toString();

		}
	}

	public String getPlanSQL() {
		StringBuilder buff = new StringBuilder();
		buff.append("DELETE FROM ");
		buff.append(tableFilter.getPlanSQL(false));
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
			queryProxy = queryProxyManager.getQueryProxy(table.getFullName());

			if (queryProxy == null) {
				queryProxy = QueryProxy.getQueryProxyAndLock(table,
						LockType.WRITE, session.getDatabase());
			}
			queryProxyManager.addProxy(queryProxy);
		} else {
			queryProxyManager.addProxy(QueryProxy.getDummyQueryProxy(session.getDatabase()
					.getLocalDatabaseInstanceInWrapper()));
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.h2.command.Prepared#shouldBePropagated()
	 */
	@Override
	public boolean shouldBePropagated() {
		/*
		 * If this is not a regular table (i.e. it is a meta-data table, then it
		 * will not be propagated regardless.
		 */
		return isRegularTable();
	}

}
