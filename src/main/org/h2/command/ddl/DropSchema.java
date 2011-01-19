/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2o.db.id.TableInfo;
import org.h2o.util.exceptions.MovedException;

import uk.ac.standrews.cs.nds.rpc.RPCException;

/**
 * This class represents the statement DROP SCHEMA
 */
public class DropSchema extends DefineCommand {

    private String schemaName;

    private boolean ifExists;

    public DropSchema(final Session session) {

        super(session);
    }

    public void setSchemaName(final String name) {

        schemaName = name;
    }

    @Override
    public int update() throws SQLException, RPCException {

        session.getUser().checkAdmin();
        session.commit(true);
        final Database db = session.getDatabase();
        final Schema schema = db.findSchema(schemaName);
        if (schema == null) {
            if (!ifExists) { throw Message.getSQLException(ErrorCode.SCHEMA_NOT_FOUND_1, schemaName); }
        }
        else {
            if (!schema.canDrop()) { throw Message.getSQLException(ErrorCode.SCHEMA_CAN_NOT_BE_DROPPED_1, schemaName); }
            db.removeDatabaseObject(session, schema);

            try {
                db.getSystemTable().removeTableInformation(new TableInfo(null, schemaName));
            }
            catch (final MovedException e) {
                throw new RPCException("System Table has moved.");
            }

        }
        return 0;
    }

    public void setIfExists(final boolean ifExists) {

        this.ifExists = ifExists;
    }

}
