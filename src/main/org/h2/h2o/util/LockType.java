package org.h2.h2o.util;

/**
 * The type of lock granted for a given query.
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
