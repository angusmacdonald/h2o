/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.util.ObjectArray;
import org.h2.util.ObjectUtils;

/**
 * A possible query execution plan. The time required to execute a query depends
 * on the order the tables are accessed.
 */
public class Plan {
	private final TableFilter[] filters;
	private final HashMap planItems = new HashMap();
	private final Expression[] allConditions;
	private final TableFilter[] allFilters;

	/**
	 * Create a query plan with the given order.
	 *
	 * @param filters the tables of the query
	 * @param count the number of table items
	 * @param condition the condition in the WHERE clause
	 */
	public Plan(TableFilter[] filters, int count, Expression condition) {
		this.filters = new TableFilter[count];
		ObjectUtils.arrayCopy(filters, this.filters, count);
		ObjectArray allCond = new ObjectArray();
		ObjectArray all = new ObjectArray();
		if (condition != null) {
			allCond.add(condition);
		}
		for (int i = 0; i < count; i++) {
			TableFilter f = filters[i];
			do {
				all.add(f);
				if (f.getJoinCondition() != null) {
					allCond.add(f.getJoinCondition());
				}
				f = f.getJoin();
			} while(f != null);
		}
		allConditions = new Expression[allCond.size()];
		allCond.toArray(allConditions);
		allFilters = new TableFilter[all.size()];
		all.toArray(allFilters);
	}

	/**
	 * Get the plan item for the given table.
	 *
	 * @param filter the table
	 * @return the plan item
	 */
	public PlanItem getItem(TableFilter filter) {
		return (PlanItem) planItems.get(filter);
	}

	/**
	 * The the list of tables.
	 *
	 * @return the list of tables
	 */
	public TableFilter[] getFilters() {
		return filters;
	}

	/**
	 * Remove all index conditions that can not be used.
	 */
	public void removeUnusableIndexConditions() {
		for (int i = 0; i < allFilters.length; i++) {
			TableFilter f = allFilters[i];
			setEvaluatable(f, true);
			if (i < allFilters.length - 1) {
				// the last table doesn't need the optimization,
				// otherwise the expression is calculated twice unnecessarily
				// (not that bad but not optimal)
				f.optimizeFullCondition(false);
			}
			f.removeUnusableIndexConditions();
		}
		for (int i = 0; i < allFilters.length; i++) {
			TableFilter f = allFilters[i];
			setEvaluatable(f, false);
		}
	}

	/**
	 * Calculate the cost of this query plan.
	 *
	 * @param session the session
	 * @return the cost
	 */
	public double calculateCost(Session session) throws SQLException {
		double cost = 1;
		boolean invalidPlan = false;
		for (int i = 0; i < allFilters.length; i++) {
			TableFilter tableFilter = allFilters[i];
			PlanItem item = tableFilter.getBestPlanItem(session);
			planItems.put(tableFilter, item);
			cost += cost * item.cost;
			setEvaluatable(tableFilter, true);
			Expression on = tableFilter.getJoinCondition();
			if (on != null) {
				if (!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
					invalidPlan = true;
					break;
				}
			}
		}
		if (invalidPlan) {
			cost = Double.POSITIVE_INFINITY;
		}
		for (int i = 0; i < allFilters.length; i++) {
			setEvaluatable(allFilters[i], false);
		}
		return cost;
	}

	private void setEvaluatable(TableFilter filter, boolean b) {
		for (int j = 0; j < allConditions.length; j++) {
			allConditions[j].setEvaluatable(filter, b);
		}
	}
}
