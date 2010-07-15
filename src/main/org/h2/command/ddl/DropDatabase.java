/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.Set;

import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.h2o.manager.ISystemTableReference;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.table.Table;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * DROP ALL OBJECTS
 */
public class DropDatabase extends DefineCommand {

	private boolean dropAllObjects;
	private boolean deleteFiles;

	public DropDatabase(Session session) {
		super(session);
	}

	public int update() throws SQLException {
		if (dropAllObjects) {
			dropAllObjects();
		}
		if (deleteFiles) {
			session.getDatabase().setDeleteFilesOnDisconnect(true);
		}
		return 0;
	}

	private void dropAllObjects() throws SQLException {
		session.getUser().checkAdmin();
		session.commit(true);
		Database db = session.getDatabase();
		ObjectArray objects;

		/*
		 * Drop everything from the System Table.
		 */
		ISystemTableReference systemTableReference = db.getSystemTableReference();

		try {
			systemTableReference.removeAllTableInformation();
		} catch (Exception e) {
			throw new SQLException(e.getMessage());
		}
		//**************************************

		// TODO local temp tables are not removed
		objects = db.getAllSchemas();
		for (int i = 0; i < objects.size(); i++) {
			Schema schema = (Schema) objects.get(i);
			if (schema.canDrop() && !schema.getName().startsWith(Constants.H2O_SCHEMA)) {
				db.removeDatabaseObject(session, schema);
			}
		}

		Set<Table> tables = db.getAllReplicas();
		for (Table t: tables) {
			if (t.getName() != null && t.getName().startsWith(Constants.H2O_SCHEMA)) continue;
			if (t.getName() != null && Table.VIEW.equals(t.getTableType())) {
				db.removeSchemaObject(session, t);
			}
		}
		for (Table t: tables) {
			if (t.getName() != null && t.getName().startsWith(Constants.H2O_SCHEMA)) continue;
			if (t.getName() != null && Table.TABLE_LINK.equals(t.getTableType())) {
				db.removeSchemaObject(session, t);
			}
		}
		for (Table t: tables) {
			if (t.getName() != null && t.getName().startsWith(Constants.H2O_SCHEMA)) continue;
			if (t.getName() != null && Table.TABLE.equals(t.getTableType())) {
				db.removeSchemaObject(session, t);
			}
		}


		session.findLocalTempTable(null);
		objects = db.getAllSchemaObjects(DbObject.SEQUENCE);
		// maybe constraints and triggers on system schemi will be allowed in
		// the future
		objects.addAll(db.getAllSchemaObjects(DbObject.CONSTRAINT));
		objects.addAll(db.getAllSchemaObjects(DbObject.TRIGGER));
		objects.addAll(db.getAllSchemaObjects(DbObject.CONSTANT));
		for (int i = 0; i < objects.size(); i++) {
			SchemaObject obj = (SchemaObject) objects.get(i);
			if (!obj.getSchema().getName().startsWith(Constants.H2O_SCHEMA)){
				db.removeSchemaObject(session, obj);
			}
		}
		objects = db.getAllUsers();
		for (int i = 0; i < objects.size(); i++) {
			User user = (User) objects.get(i);
			if (user != session.getUser()) {
				db.removeDatabaseObject(session, user);
			}
		}
		objects = db.getAllRoles();
		for (int i = 0; i < objects.size(); i++) {
			Role role = (Role) objects.get(i);
			String sql = role.getCreateSQL();
			// the role PUBLIC must not be dropped
			if (sql != null) {
				db.removeDatabaseObject(session, role);
			}
		}
		objects = db.getAllRights();
		objects.addAll(db.getAllFunctionAliases());
		objects.addAll(db.getAllAggregates());
		objects.addAll(db.getAllUserDataTypes());
		for (int i = 0; i < objects.size(); i++) {
			DbObject obj = (DbObject) objects.get(i);
			String sql = obj.getCreateSQL();
			// the role PUBLIC must not be dropped
			if (sql != null) {
				db.removeDatabaseObject(session, obj);
			}
		}
	}

	public void setDropAllObjects(boolean b) {
		this.dropAllObjects = b;
	}

	public void setDeleteFiles(boolean b) {
		this.deleteFiles = b;
	}

}
