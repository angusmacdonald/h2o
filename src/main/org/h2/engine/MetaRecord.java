/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import java.util.Comparator;

import org.h2.api.DatabaseEventListener;
import org.h2.command.Prepared;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.SearchRow;
import org.h2.schema.SchemaObject;
import org.h2.util.ObjectArray;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;
import org.h2o.db.query.TableProxyManager;

/**
 * A record in the system table of the database. It contains the SQL statement to create the database object.
 */
public class MetaRecord {

    private int id;

    private int objectType;

    private int headPos;

    private String sql;

    public MetaRecord(SearchRow r) throws SQLException {

        id = r.getValue(0).getInt();
        headPos = r.getValue(1).getInt();
        objectType = r.getValue(2).getInt();
        sql = r.getValue(3).getString();
    }

    MetaRecord(DbObject obj) {

        id = obj.getId();
        objectType = obj.getType();
        headPos = obj.getHeadPos();
        sql = obj.getCreateSQL();
    }

    MetaRecord(int id, int objectType, int headPos, String sql) {

        this.id = id;
        this.objectType = objectType;
        this.headPos = headPos;
        this.sql = sql;
    }

    /**
     * Sort the list of meta records by 'create order'.
     * 
     * @param records
     *            the list of meta records
     */
    public static void sort(ObjectArray records) {

        records.sort(new Comparator() {

            public int compare(Object o1, Object o2) {

                MetaRecord m1 = (MetaRecord) o1;
                MetaRecord m2 = (MetaRecord) o2;
                int c1 = DbObjectBase.getCreateOrder(m1.getObjectType());
                int c2 = DbObjectBase.getCreateOrder(m2.getObjectType());
                if (c1 != c2) { return c1 - c2; }
                return m1.getId() - m2.getId();
            }
        });
    }

    void setRecord(SearchRow r) {

        r.setValue(0, ValueInt.get(id));
        r.setValue(1, ValueInt.get(headPos));
        r.setValue(2, ValueInt.get(objectType));
        r.setValue(3, ValueString.get(sql));
    }

    /**
     * Execute the meta data statement.
     * 
     * @param db
     *            the database
     * @param systemSession
     *            the system session
     * @param listener
     *            the database event listener
     * @param proxyManager
     */
    void execute(Database db, Session systemSession, DatabaseEventListener listener, TableProxyManager proxyManager) throws SQLException {

        try {
            Prepared command = systemSession.prepare(sql);
            command.setObjectId(id);
            command.setHeadPos(headPos);
            command.setStartup(true);
            command.acquireLocks(proxyManager);
            command.update();

        }
        catch (Exception e) {
            SQLException s = Message.addSQL(Message.convert(e), sql);
            db.getTrace(Trace.DATABASE).error(sql, s);
            if (listener != null) {
                listener.exceptionThrown(s, sql);
                // continue startup in this case
            }
            else {
                throw s;
            }
        }
    }

    /**
     * Undo a metadata change.
     * 
     * @param db
     *            the database
     * @param systemSession
     *            the system session
     * @param listener
     *            the database event listener
     */
    void undo(Database db, Session systemSession, DatabaseEventListener listener) throws SQLException {

        try {
            DbObject obj = db.getDbObject(id);
            // null if it was already removed
            // (a identity sequence is removed when the table is removed)
            if (obj != null) {
                if (obj instanceof SchemaObject) {
                    db.removeSchemaObject(systemSession, (SchemaObject) obj);
                }
                else {
                    db.removeDatabaseObject(systemSession, obj);
                }
            }
        }
        catch (Exception e) {
            SQLException s = Message.addSQL(Message.convert(e), sql);
            db.getTrace(Trace.DATABASE).error(sql, s);
            if (listener != null) {
                listener.exceptionThrown(s, sql);
                // continue startup in this case
            }
            else {
                throw s;
            }
        }
    }

    public int getId() {

        return id;
    }

    public int getObjectType() {

        return objectType;
    }

    public String getSQL() {

        return sql;
    }

}
