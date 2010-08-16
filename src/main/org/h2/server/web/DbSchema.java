/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server.web;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Contains meta data information about a database schema. This class is used by
 * the H2 Console.
 */
public class DbSchema {

	/**
	 * Up to this many tables, the column type and indexes are listed.
	 */
	static final int MAX_TABLES_LIST_INDEXES = 100;

	/**
	 * Up to this many tables, the column names are listed.
	 */
	static final int MAX_TABLES_LIST_COLUMNS = 500;

	/**
	 * The database content container.
	 */
	DbContents contents;

	/**
	 * The schema name.
	 */
	String name;

	/**
	 * The quoted schema name.
	 */
	String quotedName;

	/**
	 * The table list.
	 */
	Map<String, Set<DbTableOrView>> tables;

	/**
	 * True if this is the default schema for this database.
	 */
	boolean isDefault;

	DbSchema(DbContents contents, String name, boolean isDefault) {
		this.contents = contents;
		this.name = name;
		this.quotedName = contents.quoteIdentifier(name);
		this.isDefault = isDefault;

		tables = new HashMap<String, Set<DbTableOrView>>();
	}

	/**
	 * Read all tables for this schema from the database meta data.
	 * 
	 * @param meta
	 *            the database meta data
	 * @param tableTypes
	 *            the table types to read
	 */
	void readTables(DatabaseMetaData meta, String[] tableTypes)
			throws SQLException {
		ResultSet rs = meta.getTables(null, name, null, tableTypes);

		while (rs.next()) {
			DbTableOrView table = new DbTableOrView(this, rs);
			if (contents.isOracle && table.name.indexOf('$') > 0) {
				continue;
			}

			if (tables.get(table.name) != null) {
				Set<DbTableOrView> set = tables.get(table.name);
				set.add(table);
			} else {
				Set<DbTableOrView> set = new HashSet<DbTableOrView>();
				set.add(table);
				tables.put(table.name, set);
			}
		}
		rs.close();

		if (tables.size() < MAX_TABLES_LIST_COLUMNS) {
			for (Set<DbTableOrView> tableSet : tables.values()) {
				for (DbTableOrView dbtab : tableSet) {
					dbtab.readColumns(meta);
				}
			}
		}
	}

}
