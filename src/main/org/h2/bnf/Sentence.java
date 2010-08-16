/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;
import java.util.HashSet;

import org.h2.server.web.DbSchema;
import org.h2.server.web.DbTableOrView;
import org.h2.util.StringUtils;

/**
 * A query context object. It contains the list of table and alias objects. Used
 * for autocomplete.
 */
public class Sentence {

	/**
	 * This token type means the possible choices of the item depend on the
	 * context. For example the item represents a table name of the current
	 * database.
	 */
	public static final int CONTEXT = 0;

	/**
	 * The token type for a keyword.
	 */
	static final int KEYWORD = 1;

	/**
	 * The token type for a function name.
	 */
	static final int FUNCTION = 2;

	private static final long MAX_PROCESSING_TIME = 100;

	/**
	 * The map of next tokens in the form type#tokenName token.
	 */
	private HashMap next = new HashMap();

	/**
	 * The complete query string.
	 */
	private String query;

	/**
	 * The uppercase version of the query string.
	 */
	private String queryUpper;

	private long stopAt;
	private DbSchema lastMatchedSchema;
	private DbTableOrView lastMatchedTable;
	private DbTableOrView lastTable;
	private HashSet tables;
	private HashMap aliases;

	/**
	 * Start the timer to make sure processing doesn't take too long.
	 */
	void start() {
		stopAt = System.currentTimeMillis() + MAX_PROCESSING_TIME;
	}

	/**
	 * Check if it's time to stop processing. Processing auto-complete shouldn't
	 * take more than a few milliseconds.
	 * 
	 * @return true if it's time to stop processing
	 */
	boolean shouldStop() {
		return System.currentTimeMillis() > stopAt;
	}

	/**
	 * Add a word to the set of next tokens.
	 * 
	 * @param n
	 *            the token name
	 * @param string
	 *            an example text
	 * @param type
	 *            the token type
	 */
	public void add(String n, String string, int type) {
		next.put(type + "#" + n, string);
	}

	/**
	 * Add an alias name and object
	 * 
	 * @param alias
	 *            the alias name
	 * @param table
	 *            the alias table
	 */
	public void addAlias(String alias, DbTableOrView table) {
		if (aliases == null) {
			aliases = new HashMap();
		}
		aliases.put(alias, table);
	}

	/**
	 * Add a table.
	 * 
	 * @param table
	 *            the table
	 */
	public void addTable(DbTableOrView table) {
		lastTable = table;
		if (tables == null) {
			tables = new HashSet();
		}
		tables.add(table);
	}

	/**
	 * Get the set of tables.
	 * 
	 * @return the set of tables
	 */
	public HashSet getTables() {
		return tables;
	}

	/**
	 * Get the alias map.
	 * 
	 * @return the alias map
	 */
	public HashMap getAliases() {
		return aliases;
	}

	/**
	 * Get the last added table.
	 * 
	 * @return the last table
	 */
	public DbTableOrView getLastTable() {
		return lastTable;
	}

	/**
	 * Get the last matched schema if the last match was a schema.
	 * 
	 * @return the last schema or null
	 */
	public DbSchema getLastMatchedSchema() {
		return lastMatchedSchema;
	}

	/**
	 * Set the last matched schema if the last match was a schema, or null if it
	 * was not.
	 * 
	 * @param schema
	 *            the last matched schema or null
	 */
	public void setLastMatchedSchema(DbSchema schema) {
		this.lastMatchedSchema = schema;
	}

	/**
	 * Set the last matched table if the last match was a table.
	 * 
	 * @param table
	 *            the last matched table or null
	 */
	public void setLastMatchedTable(DbTableOrView table) {
		this.lastMatchedTable = table;
	}

	/**
	 * Get the last matched table if the last match was a table.
	 * 
	 * @return the last table or null
	 */
	public DbTableOrView getLastMatchedTable() {
		return lastMatchedTable;
	}

	/**
	 * Set the query string.
	 * 
	 * @param query
	 *            the query string
	 */
	public void setQuery(String query) {
		if (!StringUtils.equals(this.query, query)) {
			this.query = query;
			this.queryUpper = StringUtils.toUpperEnglish(query);
		}
	}

	/**
	 * Get the query string.
	 * 
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * Get the uppercase version of the query string.
	 * 
	 * @return the uppercase query
	 */
	public String getQueryUpper() {
		return queryUpper;
	}

	/**
	 * Get the map of next tokens.
	 * 
	 * @return the next token map
	 */
	public HashMap getNext() {
		return next;
	}

}
