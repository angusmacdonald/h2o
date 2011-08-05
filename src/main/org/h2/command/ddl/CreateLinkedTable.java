/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.constant.LocationPreference;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.schema.Schema;
import org.h2.table.Table;
import org.h2.table.TableLink;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * This class represents the statement CREATE LINKED TABLE
 */
public class CreateLinkedTable extends SchemaCommand {

    private String tableName;

    private String driver, url, user, password, originalSchema, originalTable;

    private boolean ifNotExists;

    private String comment;

    private boolean emitUpdates;

    private boolean force;

    private boolean temporary;

    private boolean globalTemporary;

    private boolean readOnly;

    public CreateLinkedTable(final Session session, final Schema schema) {

        super(session, schema);
    }

    public void setTableName(final String tableName) {

        this.tableName = tableName;
    }

    public void setDriver(final String driver) {

        this.driver = driver;
    }

    public void setOriginalTable(final String originalTable) {

        this.originalTable = originalTable;
    }

    public void setPassword(final String password) {

        this.password = password;
    }

    public void setUrl(final String url) {

        this.url = url;
    }

    public void setUser(final String user) {

        this.user = user;
    }

    public void setIfNotExists(final boolean ifNotExists) {

        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() throws SQLException {

        session.commit(true);
        final Database db = session.getDatabase();
        session.getUser().checkAdmin();
        final Schema schema = getSchema();
        if (schema.findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE) != null) {
            if (ifNotExists) { return 0; }

            //If an existing linkedtable is there remove it, because we have no way currently of specifying which linked table to query.
            Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Removing existing linked table for " + tableName);

            final Table linkedTableToRemove = schema.findLocalTableOrView(session, tableName);

            final Set<String> urls = new HashSet<String>();
            urls.add(url);
            final boolean alreadyExists = schema.removeLinkedTable(linkedTableToRemove, urls);

            if (alreadyExists) {
                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Linked Table already exists to correct location.");

                return 0;
            }

        }
        final int id = getObjectId(false, true);
        final TableLink table = schema.createTableLink(id, tableName, driver, url, user, password, originalSchema, originalTable, emitUpdates, force);
        table.setTemporary(temporary);
        table.setGlobalTemporary(globalTemporary);
        table.setComment(comment);
        table.setReadOnly(readOnly);
        if (temporary && !globalTemporary) {
            session.addLocalTempTable(table);
        }
        else {
            db.addSchemaObject(session, table);
        }
        return 0;
    }

    public void setEmitUpdates(final boolean emitUpdates) {

        this.emitUpdates = emitUpdates;
    }

    public void setComment(final String comment) {

        this.comment = comment;
    }

    public void setForce(final boolean force) {

        this.force = force;
    }

    public void setTemporary(final boolean temp) {

        temporary = temp;
    }

    public void setGlobalTemporary(final boolean globalTemp) {

        globalTemporary = globalTemp;
    }

    public void setReadOnly(final boolean readOnly) {

        this.readOnly = readOnly;
    }

    public void setOriginalSchema(final String originalSchema) {

        this.originalSchema = originalSchema;
    }

}
