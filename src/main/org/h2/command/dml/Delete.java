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
import org.h2.expression.Expression;
import org.h2.h2o.comms.QueryProxy;
import org.h2.h2o.comms.QueryProxyManager;
import org.h2.h2o.util.LockType;
import org.h2.log.UndoLogRecord;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;

/**
 * This class represents the statement
 * DELETE
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
		if (isRegularTable()){
			return queryProxy.executeUpdate(sqlStatement, transactionName, session);
		}
        
        table.fireBefore(session);
        table.lock(session, true, false);
        RowList rows = new RowList(session);
        try {
            setCurrentRowNumber(0);
            while (tableFilter.next()) {
                checkCanceled();
                setCurrentRowNumber(rows.size() + 1);
                if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
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

    public String getPlanSQL() {
        StringBuffer buff = new StringBuffer();
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
    
    /* (non-Javadoc)
	 * @see org.h2.command.Prepared#acquireLocks()
	 */
	@Override
	public QueryProxy acquireLocks(QueryProxyManager queryProxyManager) throws SQLException  {
		/*
		 * (QUERY PROPAGATED TO ALL REPLICAS).
		 */
		if (isRegularTable()){
			queryProxy = queryProxyManager.getQueryProxy(table.getFullName());

			if (queryProxy == null){
			queryProxy = QueryProxy.getQueryProxyAndLock(table, LockType.WRITE, session.getDatabase());
			}
			return queryProxy;
		}
		
		return QueryProxy.getDummyQueryProxy(session.getDatabase().getLocalDatabaseInstance());
		
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
	
	
}
