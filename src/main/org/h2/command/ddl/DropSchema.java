/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.rmi.RemoteException;
import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.util.exceptions.MovedException;

/**
 * This class represents the statement DROP SCHEMA
 */
public class DropSchema extends DefineCommand {

	private String schemaName;
	private boolean ifExists;

	public DropSchema(Session session) {
		super(session);
	}

	public void setSchemaName(String name) {
		this.schemaName = name;
	}

	public int update() throws SQLException, RemoteException {
		session.getUser().checkAdmin();
		session.commit(true);
		Database db = session.getDatabase();
		Schema schema = db.findSchema(schemaName);
		if (schema == null) {
			if (!ifExists) {
				throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1,
						schemaName);
			}
		} else {
			if (!schema.canDrop()) {
				throw Message.getSQLException(
						ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, schemaName);
			}
			db.removeDatabaseObject(session, schema);

			if (Constants.IS_H2O) {
				try {
					db.getSystemTable().removeTableInformation(
							new TableInfo(null, schemaName));
				} catch (MovedException e) {
					throw new RemoteException("System Table has moved.");
				}
			}
		}
		return 0;
	}

	public void setIfExists(boolean ifExists) {
		this.ifExists = ifExists;
	}

}
