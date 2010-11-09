/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.schema;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.DbObjectBase;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.table.ReplicaSet;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.table.TableLink;
import org.h2.util.ObjectArray;

/**
 * A schema as created by the SQL statement CREATE SCHEMA
 */
public class Schema extends DbObjectBase {

    private User owner;

    private final boolean system;

    private final Map<String, ReplicaSet> tablesAndViews = new HashMap<String, ReplicaSet>();

    private final Map indexes = new HashMap();

    private final Map sequences = new HashMap();

    private final Map triggers = new HashMap();

    private final Map constraints = new HashMap();

    private final Map constants = new HashMap();

    /**
     * The set of returned unique names that are not yet stored. It is used to avoid returning the same unique name twice when multiple
     * threads concurrently create objects.
     */
    private final HashSet temporaryUniqueNames = new HashSet();

    /**
     * Create a new schema object.
     * 
     * @param database
     *            the database
     * @param id
     *            the object id
     * @param schemaName
     *            the schema name
     * @param owner
     *            the owner of the schema
     * @param system
     *            if this is a system schema (such a schema can not be dropped)
     */
    public Schema(final Database database, final int id, final String schemaName, final User owner, final boolean system) {

        initDbObjectBase(database, id, schemaName, Trace.SCHEMA);
        this.owner = owner;
        this.system = system;
    }

    /**
     * Check if this schema can be dropped. System schemas can not be dropped.
     * 
     * @return true if it can be dropped
     */
    public boolean canDrop() {

        return !system;
    }

    @Override
    public String getCreateSQLForCopy(final Table table, final String quotedName) {

        throw Message.throwInternalError();
    }

    @Override
    public String getDropSQL() {

        return null;
    }

    @Override
    public String getCreateSQL() {

        if (system) { return null; }
        final StringBuilder buff = new StringBuilder();
        buff.append("CREATE SCHEMA IF NOT EXISTS ");
        buff.append(getSQL());
        buff.append(" AUTHORIZATION ");
        buff.append(owner.getSQL());
        return buff.toString();
    }

    @Override
    public int getType() {

        return DbObject.SCHEMA;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        while (triggers != null && triggers.size() > 0) {
            final TriggerObject obj = (TriggerObject) triggers.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (constraints != null && constraints.size() > 0) {
            final Constraint obj = (Constraint) constraints.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (tablesAndViews != null && tablesAndViews.size() > 0) {
            final ReplicaSet replicaSet = (ReplicaSet) tablesAndViews.values().toArray()[0];
            final Set<Table> tables = replicaSet.getAllCopies();
            final Table[] array = tables.toArray(new Table[0]);

            for (final Table table : array) {
                database.removeSchemaObject(session, table);
            }
            replicaSet.removeAllCopies();
            tablesAndViews.remove(replicaSet.getTableName());

        }
        while (indexes != null && indexes.size() > 0) {
            final Index obj = (Index) indexes.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (sequences != null && sequences.size() > 0) {
            final Sequence obj = (Sequence) sequences.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        while (constants != null && constants.size() > 0) {
            final Constant obj = (Constant) constants.values().toArray()[0];
            database.removeSchemaObject(session, obj);
        }
        database.removeMeta(session, getId());
        owner = null;
        invalidate();
    }

    @Override
    public void checkRename() {

        // ok
    }

    /**
     * Get the owner of this schema.
     * 
     * @return the owner
     */
    public User getOwner() {

        return owner;
    }

    private Map getMap(final int type) {

        switch (type) {
            case DbObject.TABLE_OR_VIEW:
                return tablesAndViews;
            case DbObject.SEQUENCE:
                return sequences;
            case DbObject.INDEX:
                return indexes;
            case DbObject.TRIGGER:
                return triggers;
            case DbObject.CONSTRAINT:
                return constraints;
            case DbObject.CONSTANT:
                return constants;
            default:
                throw Message.throwInternalError("type=" + type);
        }
    }

    /**
     * Add an object to this schema.
     * 
     * @param obj
     *            the object to add
     */
    public void add(final SchemaObject obj) {

        if (SysProperties.CHECK && obj.getSchema() != this) {
            Message.throwInternalError("wrong schema");
        }
        final String name = obj.getName();
        final int type = obj.getType();
        final Map map = getMap(type);

        if (SysProperties.CHECK && map.get(name) != null && type != DbObject.TABLE_OR_VIEW) {
            Message.throwInternalError("object already exists");
        }
        if (type == DbObject.TABLE_OR_VIEW) {
            // H2O. Do something special - this has to be added to a replica set
            // if one doesn't exist.

            if (tablesAndViews.get(name) == null) {
                tablesAndViews.put(name, new ReplicaSet(obj));
            }
            else {
                final ReplicaSet replicaSet = tablesAndViews.get(name);
                replicaSet.addNewReplica(obj); // XXX assumes this will never
                                               // try to over-write an existing
                                               // table.
                tablesAndViews.put(name, replicaSet);
            }
        }
        else {
            map.put(name, obj);
        }
        freeUniqueName(name);
    }

    /**
     * Rename an object.
     * 
     * @param obj
     *            the object to rename
     * @param newName
     *            the new name
     */
    public void rename(final SchemaObject obj, final String newName) throws SQLException {

        final int type = obj.getType();
        final Map map = getMap(type);

        if (SysProperties.CHECK) {
            if (!map.containsKey(obj.getName())) {
                Message.throwInternalError("not found: " + obj.getName());
            }
            if (obj.getName().equals(newName) || map.containsKey(newName)) {
                Message.throwInternalError("object already exists: " + newName);
            }
        }
        if (type == DbObject.TABLE_OR_VIEW) {

            final Table table = (Table) obj;
            final String tableName = table.getName();
            final ReplicaSet replicaSet = tablesAndViews.get(tableName);
            replicaSet.checkRename();
            tablesAndViews.remove(tableName);

            freeUniqueName(tableName);

            replicaSet.rename(newName);

            tablesAndViews.put(newName, replicaSet);

        }
        else {
            obj.checkRename();
            map.remove(obj.getName());
            freeUniqueName(obj.getName());
            obj.rename(newName);
            map.put(newName, obj);
            freeUniqueName(newName);
        }
    }

    /**
     * Try to find a table or view with this name. This method returns null if no object with this name exists. Local temporary tables are
     * also returned.
     * 
     * @param session
     *            the session
     * @param name
     *            the object name
     * @param locale
     * @return the object or null
     */
    public Table findTableOrView(final Session session, final String name, final LocationPreference locale) {

        final ReplicaSet replicaSet = tablesAndViews.get(name);

        Table table = null;
        if (replicaSet == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        else if (replicaSet != null) {

            if (replicaSet.size() == 1 && !locale.isStrict() || locale == LocationPreference.NO_PREFERENCE) { // XXX more advanced logic to choose replica would go here.
                table = replicaSet.getACopy();
            }
            else if (locale == LocationPreference.LOCAL || locale == LocationPreference.LOCAL_STRICT) {

                table = replicaSet.getLocalCopy(); // XXX what if no local copy
                                                   // exists?
            }
            else if (locale == LocationPreference.PRIMARY || locale == LocationPreference.PRIMARY_STRICT) {
                table = replicaSet.getPrimaryCopy();
            }

        }
        return table;
    }

    /**
     * Remove a LinkedTable if one already exists and it doesn't point to the correct database URL
     * (the urlRequired parameter). This is called from CreateLinkedTable if a linked table has 
     * to be created to another URL.
     * @return Returns true if the link table required (at the correct URL) already exists).
     */
    public boolean removeLinkedTable(final SchemaObject obj, final String urlRequired) {

        final Table table = (Table) obj;
        final ReplicaSet replicaSet = tablesAndViews.get(table.getName());

        final boolean linkedTableAlreadyExists = replicaSet.removeLinkedTable(table, urlRequired);

        return linkedTableAlreadyExists;

    }

    /**
     * Try to find a LOCAL VERSION of a table or view with this name. This method returns null if no object with this name exists. Local
     * temporary tables are also returned.
     * 
     * @param session
     *            the session
     * @param name
     *            the object name
     * @return the object or null
     */
    public Table findLocalTableOrView(final Session session, final String name) {

        final ReplicaSet replicaSet = tablesAndViews.get(name);

        Table table = null;
        if (replicaSet == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        else {
            return replicaSet.getLocalCopy();
        }
        return table;
    }

    /**
     * Try to find an index with this name. This method returns null if no object with this name exists.
     * 
     * @param session
     *            the session
     * @param name
     *            the object name
     * @return the object or null
     */
    public Index findIndex(final Session session, final String name) {

        Index index = (Index) indexes.get(name);
        if (index == null) {
            index = session.findLocalTempTableIndex(name);
        }
        return index;
    }

    /**
     * Try to find a trigger with this name. This method returns null if no object with this name exists.
     * 
     * @param name
     *            the object name
     * @return the object or null
     */
    public TriggerObject findTrigger(final String name) {

        return (TriggerObject) triggers.get(name);
    }

    /**
     * Try to find a sequence with this name. This method returns null if no object with this name exists.
     * 
     * @param sequenceName
     *            the object name
     * @return the object or null
     */
    public Sequence findSequence(final String sequenceName) {

        return (Sequence) sequences.get(sequenceName);
    }

    /**
     * Try to find a constraint with this name. This method returns null if no object with this name exists.
     * 
     * @param session
     *            the session
     * @param name
     *            the object name
     * @return the object or null
     */
    public Constraint findConstraint(final Session session, final String name) {

        Constraint constraint = (Constraint) constraints.get(name);
        if (constraint == null) {
            constraint = session.findLocalTempTableConstraint(name);
        }
        return constraint;
    }

    /**
     * Try to find a user defined constant with this name. This method returns null if no object with this name exists.
     * 
     * @param constantName
     *            the object name
     * @return the object or null
     */
    public Constant findConstant(final String constantName) {

        return (Constant) constants.get(constantName);
    }

    /**
     * Release a unique object name.
     * 
     * @param name
     *            the object name
     */
    public void freeUniqueName(final String name) {

        if (name != null) {
            synchronized (temporaryUniqueNames) {
                temporaryUniqueNames.remove(name);
            }
        }
    }

    private String getUniqueName(final DbObject obj, final Map map, String prefix) {

        final String hash = Integer.toHexString(obj.getName().hashCode()).toUpperCase();
        String name = null;
        synchronized (temporaryUniqueNames) {
            for (int i = 1; i < hash.length(); i++) {
                name = prefix + hash.substring(0, i);
                if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                    break;
                }
                name = null;
            }
            if (name == null) {
                prefix = prefix + hash + "_";
                for (int i = 0;; i++) {
                    name = prefix + i;
                    if (!map.containsKey(name) && !temporaryUniqueNames.contains(name)) {
                        break;
                    }
                }
            }
            temporaryUniqueNames.add(name);
        }
        return name;
    }

    /**
     * Create a unique constraint name.
     * 
     * @param session
     *            the session
     * @param table
     *            the constraint table
     * @return the unique name
     */
    public String getUniqueConstraintName(final Session session, final Table table) {

        Map tableConstraints;
        if (table.getTemporary() && !table.getGlobalTemporary()) {
            tableConstraints = session.getLocalTempTableConstraints();
        }
        else {
            tableConstraints = constraints;
        }
        return getUniqueName(table, tableConstraints, "CONSTRAINT_");
    }

    /**
     * Create a unique index name.
     * 
     * @param session
     *            the session
     * @param table
     *            the indexed table
     * @param prefix
     *            the index name prefix
     * @return the unique name
     */
    public String getUniqueIndexName(final Session session, final Table table, final String prefix) {

        Map tableIndexes;
        if (table.getTemporary() && !table.getGlobalTemporary()) {
            tableIndexes = session.getLocalTempTableIndexes();
        }
        else {
            tableIndexes = indexes;
        }
        return getUniqueName(table, tableIndexes, prefix);
    }

    /**
     * Get the table or view with the given name. Local temporary tables are also returned.
     * 
     * @param session
     *            the session
     * @param name
     *            the table or view name
     * @return the table or view
     * @throws SQLException
     *             if no such object exists
     */
    public Table getTableOrView(final Session session, final String name) throws SQLException {

        final ReplicaSet tables = tablesAndViews.get(name);

        Table table;
        if (tables == null && session != null) {
            table = session.findLocalTempTable(name);
        }
        else {
            table = tables.getACopy();
        }

        return table;
    }

    public ReplicaSet getTablesOrViews(final Session session, final String name) throws SQLException {

        final ReplicaSet tables = tablesAndViews.get(name);

        return tables;
    }

    /**
     * Get the index with the given name.
     * 
     * @param name
     *            the index name
     * @return the index
     * @throws SQLException
     *             if no such object exists
     */
    public Index getIndex(final String name) throws SQLException {

        final Index index = (Index) indexes.get(name);
        if (index == null) { throw Message.getSQLException(ErrorCode.INDEX_NOT_FOUND_1, name); }
        return index;
    }

    /**
     * Get the constraint with the given name.
     * 
     * @param name
     *            the constraint name
     * @return the constraint
     * @throws SQLException
     *             if no such object exists
     */
    public Constraint getConstraint(final String name) throws SQLException {

        final Constraint constraint = (Constraint) constraints.get(name);
        if (constraint == null) { throw Message.getSQLException(ErrorCode.CONSTRAINT_NOT_FOUND_1, name); }
        return constraint;
    }

    /**
     * Get the user defined constant with the given name.
     * 
     * @param constantName
     *            the constant name
     * @return the constant
     * @throws SQLException
     *             if no such object exists
     */
    public Constant getConstant(final String constantName) throws SQLException {

        final Constant constant = (Constant) constants.get(constantName);
        if (constant == null) { throw Message.getSQLException(ErrorCode.CONSTANT_NOT_FOUND_1, constantName); }
        return constant;
    }

    /**
     * Get the sequence with the given name.
     * 
     * @param sequenceName
     *            the sequence name
     * @return the sequence
     * @throws SQLException
     *             if no such object exists
     */
    public Sequence getSequence(final String sequenceName) throws SQLException {

        final Sequence sequence = (Sequence) sequences.get(sequenceName);
        if (sequence == null) { throw Message.getSQLException(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName); }
        return sequence;
    }

    /**
     * Get all objects of the given type.
     * 
     * @param type
     *            the object type
     * @return a (possible empty) list of all objects
     */
    public ObjectArray getAll(final int type) {

        final Map map = getMap(type);
        return new ObjectArray(map.values());
    }

    /**
     * Remove an object from this schema.
     * 
     * @param obj
     *            the object to remove
     */
    public void remove(final SchemaObject obj) {

        final String objName = obj.getName();
        final Map map = getMap(obj.getType());

        if (SysProperties.CHECK && !map.containsKey(objName)) {
            Message.throwInternalError("not found: " + objName);
        }
        if (obj.getType() == DbObject.TABLE_OR_VIEW) {

            final Table table = (Table) obj;
            final ReplicaSet replicaSet = tablesAndViews.get(table.getName());

            final boolean inUse = replicaSet.removeCopy(table);

            if (!inUse) {
                // Delete this replicaSet
                tablesAndViews.remove(table.getName());
                // System.out.println("H2O. Removing replica-set for table '" +
                // table.getName() + "'.");
            }
        }
        else {
            map.remove(objName);
        }
        freeUniqueName(objName);
    }

    /**
     * Add a table to the schema.
     * 
     * @param tableName
     *            the table name
     * @param id
     *            the object id
     * @param columns
     *            the column list
     * @param persistent
     *            if the table should be persistent
     * @param clustered
     *            if a clustered table should be created
     * @param headPos
     *            the position (page number) of the head
     * @return the created {@link TableData} object
     */
    public TableData createTable(final String tableName, final int id, final ObjectArray columns, final boolean persistent, final boolean clustered, final int headPos) throws SQLException {

        return new TableData(this, tableName, id, columns, persistent, clustered, headPos);
    }

    /**
     * Add a linked table to the schema.
     * 
     * @param id
     *            the object id
     * @param tableName
     *            the table name of the alias
     * @param driver
     *            the driver class name
     * @param url
     *            the database URL
     * @param user
     *            the user name
     * @param password
     *            the password
     * @param originalSchema
     *            the schema name of the target table
     * @param originalTable
     *            the table name of the target table
     * @param emitUpdates
     *            if updates should be emitted instead of delete/insert
     * @param force
     *            create the object even if the database can not be accessed
     * @return the {@link TableLink} object
     */
    public TableLink createTableLink(final int id, final String tableName, final String driver, final String url, final String user, final String password, final String originalSchema, final String originalTable, final boolean emitUpdates, final boolean force) throws SQLException {

        return new TableLink(this, id, tableName, driver, url, user, password, originalSchema, originalTable, emitUpdates, force);
    }

    /**
     * @return the tablesAndViews
     */
    public Map<String, ReplicaSet> getTablesAndViews() {

        return tablesAndViews;
    }

}
