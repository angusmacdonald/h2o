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
package org.h2o.db.query.locking;

/**
 * The type of lock granted for a given query.
 * 
 * @author angus
 * 
 */
public enum LockType {

	/*
	 * Permission to execute a SELECT statement (with no updates).
	 */
	READ,

	/*
	 * Permission to execute an update (other than CREATE TABLE, CREATE SCHEMA).
	 */
	WRITE,

	/*
	 * Permission to execute CREATE TABLE, CREATE SCHEMA.
	 */
	CREATE,

	/*
	 * No lock granted.
	 */
	NONE
}
