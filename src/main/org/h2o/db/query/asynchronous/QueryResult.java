/*
 * Copyright (C) 2009-2010 School of Computer Science, University of St Andrews. All rights reserved.
 * Project Homepage: http://blogs.cs.st-andrews.ac.uk/h2o
 *
 * H2O is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.

 * H2O is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.

 * You should have received a copy of the GNU General Public License
 * along with H2O.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.h2o.db.query.asynchronous;

import java.sql.SQLException;

import org.h2o.db.id.TableInfo;
import org.h2o.db.wrappers.DatabaseInstanceWrapper;

/**
 * Encapsulates the result of executing a query. This class is returned as the result
 * of the {@link AsynchronousQueryExecutor} class.
 *
 * @author Angus Macdonald (angus AT cs.st-andrews.ac.uk)
 */
public class QueryResult {
	private int result;

	private SQLException exception = null;

	private DatabaseInstanceWrapper wrapper;

	private TableInfo tableInfo;
	
	private int updateID;

	public QueryResult(int result, DatabaseInstanceWrapper wrapper, int updateID, TableInfo tableInfo) {
		this.result = result;
		this.wrapper = wrapper;
		this.updateID = updateID;
		this.tableInfo = tableInfo;
	}

	public QueryResult(SQLException exception, DatabaseInstanceWrapper wrapper, int updateID, TableInfo tableInfo) {
		this.exception = exception;
		this.wrapper = wrapper;
		this.updateID = updateID;
		this.tableInfo = tableInfo;
	}

	public SQLException getException() {
		return exception;
	}

	public DatabaseInstanceWrapper getWrapper() {
		return wrapper;
	}

	public int getResult() {
		return result;
	}

	public int getUpdateID(){
		return updateID;
	}
	
	public TableInfo getTable(){
		return tableInfo;
	}
}
