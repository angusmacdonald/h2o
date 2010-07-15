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
package org.h2.test.h2o;

/**
 * Represents an SQL query and its expected result.
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class TestQuery {
	private String query;
	private int[] pKey;
	private String[] secondColumn;
	private String tableName;

	/**
	 * @param query
	 * @param pKey
	 * @param secondColumn
	 */
	public TestQuery(String query, String tableName, int[] pKey, String[] secondColumn) {
		this.tableName = tableName;
		this.query = query;
		this.pKey = pKey;
		this.secondColumn = secondColumn;
	}

	/**
	 * @return the query
	 */
	public String getSQL() {
		return query;
	}
	/**
	 * @return the pKey
	 */
	public int[] getPrimaryKey() {
		return pKey;
	}
	/**
	 * @return the secondColumn
	 */
	public String[] getSecondColumn() {
		return secondColumn;
	}

	/**
	 * @return
	 */
	public String getTableName() {
		return tableName;
	}


}
