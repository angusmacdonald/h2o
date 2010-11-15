/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.h2o;

import java.rmi.RemoteException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.h2.command.Command;
import org.h2.command.Parser;
import org.h2.command.Prepared;
import org.h2.command.ddl.AlterTableAddConstraint;
import org.h2.command.ddl.CreateIndex;
import org.h2.command.ddl.SchemaCommand;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Query;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.index.IndexType;
import org.h2.jdbc.JdbcSQLException;
import org.h2.message.Message;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableLinkConnection;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2o.db.id.TableInfo;
import org.h2o.db.interfaces.TableManagerRemote;
import org.h2o.db.manager.PersistentSystemTable;
import org.h2o.db.manager.interfaces.ISystemTable;
import org.h2o.db.manager.interfaces.ISystemTableReference;
import org.h2o.util.exceptions.MovedException;
import org.h2o.viewer.H2OEventBus;
import org.h2o.viewer.gwt.client.DatabaseStates;
import org.h2o.viewer.gwt.client.H2OEvent;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * This class represents the statement CREATE REPLICA
 */
public class CreateReplica extends SchemaCommand {

    private String tableName;

    private final ObjectArray constraintCommands = new ObjectArray();

    private ObjectArray columns = new ObjectArray();

    private IndexColumn[] pkColumns;

    private boolean ifNotExists;

    private boolean persistent = true;

    private boolean temporary;

    private boolean globalTemporary;

    private boolean onCommitDrop;

    private boolean onCommitTruncate;

    private Query asQuery;

    private String comment;

    private boolean clustered;

    private TableLinkConnection conn;

    private boolean storesLowerCase = false;

    private boolean storesMixedCase = false;

    private boolean supportsMixedCaseIdentifiers = false;

    /**
     * Array containing all of the insert statements required for this replicas state to match that of the primary.
     */
    private List<String> inserts = null;

    /**
     * The intended location of the remote replica.
     */
    private String whereReplicaWillBeCreated = null;

    /**
     * The location where the data currently exists and can be copied from.
     */
    private String whereDataWillBeTakenFrom = null;

    /**
     * The next table to be replicated if it is to be done with more than one.
     */
    private CreateReplica next = null;

    private int tableSet = -1; // the set of tables which this replica will

    // belong to.
    private final boolean empty;

    private final boolean updateData;

    /**
     * 
     * @param session
     * @param schema
     * @param empty
     *            Whether the replica being created is of a table which is empty. If it is no data has to be transferred initially.
     * @param updateData
     *            Whether to update the contents of the replica even if it already exists.
     */
    public CreateReplica(final Session session, final Schema schema, final boolean empty, final boolean updateData) {

        super(session, schema);

        this.updateData = updateData;
        this.empty = empty;
    }

    public void setQuery(final Query query) {

        asQuery = query;
    }

    public void setTemporary(final boolean temporary) {

        this.temporary = temporary;
    }

    public void setTableName(final String tableName) {

        if (tableName.contains(".")) {
            this.tableName = tableName.split("\\.")[1];
        }
        else {
            this.tableName = tableName;
        }
    }

    /**
     * Add a column to this table.
     * 
     * @param column
     *            the column to add
     */
    public void addColumn(final Column column) {

        if (columns == null) {
            columns = new ObjectArray();
        }
        columns.add(column);
    }

    /**
     * Add a constraint statement to this statement. The primary key definition is one possible constraint statement.
     * 
     * @param command
     *            the statement to add
     */
    public void addConstraintCommand(final Prepared command) throws SQLException {

        if (command instanceof CreateIndex) {
            constraintCommands.add(command);
        }
        else {
            final AlterTableAddConstraint con = (AlterTableAddConstraint) command;
            boolean alreadySet;
            if (con.getType() == AlterTableAddConstraint.PRIMARY_KEY) {
                alreadySet = setPrimaryKeyColumns(con.getIndexColumns());
            }
            else {
                alreadySet = false;
            }
            if (!alreadySet) {
                constraintCommands.add(command);
            }
        }
    }

    public void setIfNotExists(final boolean ifNotExists) {

        this.ifNotExists = ifNotExists;
    }

    @Override
    public int update() throws SQLException, RemoteException {

        final Database db = session.getDatabase();

        if (whereReplicaWillBeCreated != null || db.getFullDatabasePath().equals(whereReplicaWillBeCreated)) {
            final int result = pushCommand(whereReplicaWillBeCreated, "CREATE REPLICA " + tableName + " FROM '" + whereDataWillBeTakenFrom + "'", true); // command
            // will  be executed elsewhere

            // Update the System Table here.

            if (result == 0) {
                try {
                    final ISystemTable sm = db.getSystemTable(); // db.getSystemSession()

                    final Table table = getSchema().findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE);

                    if (tableSet == -1) {
                        tableSet = 1; // sm.getTableSetNumber(new
                        // TableInfo(tableName,
                        // getSchema().getName()));
                    }
                    else {
                        if (next != null) {
                            next.setTableSet(tableSet);
                        }
                    }

                    final TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), tableSet, table.getTableType(), db.getURL());

                    sm.lookup(ti).getTableManager().addReplicaInformation(ti);
                }
                catch (final MovedException e) {
                    throw new RemoteException("System Table has moved.");
                }
            }

            return result;

        }
        else {
            readSQL(); // command will be executed here - get the table
            // meta-data and contents.
        }

        // TODO rights: what rights are required to create a table?
        session.commit(true);

        if (!db.isPersistent()) {
            persistent = false;
        }

        boolean createEntirelyNewReplica = true;
        if (getSchema().findLocalTableOrView(session, tableName) != null) { // H2O. Check  for  local version  here.
            if (ifNotExists && !updateData) {
                return 0;
            }
            else if (updateData) {
                createEntirelyNewReplica = false;
            }
            else {
                throw new SQLException("Replica already exists at this location.");
            }
        }

        final String fullTableName = getSchema().getName() + "." + tableName;
        if (!empty && getSchema().findTableOrView(session, fullTableName, LocationPreference.NO_PREFERENCE) == null) { // H2O.
            // Check for the existence of any version. if a linked table version doesn't exist we must create it.
            final String createLinkedTable = "\nCREATE LINKED TABLE IF NOT EXISTS " + fullTableName + "('org.h2.Driver', '" + whereDataWillBeTakenFrom + "', '" + PersistentSystemTable.USERNAME + "', '" + PersistentSystemTable.PASSWORD + "', '" + fullTableName + "');";
            final Parser queryParser = new Parser(session, true);
            final Command sqlQuery = queryParser.prepareCommand(createLinkedTable);
            sqlQuery.update();
        }

        Table table = null;

        if (createEntirelyNewReplica) {
            if (asQuery != null) {
                asQuery.prepare();
                if (columns.size() == 0) {
                    generateColumnsFromQuery();
                }
                else if (columns.size() != asQuery.getColumnCount()) { throw Message.getSQLException(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH); }
            }
            if (pkColumns != null) {
                final int len = pkColumns.length;
                for (int i = 0; i < columns.size(); i++) {
                    final Column c = (Column) columns.get(i);
                    for (int j = 0; j < len; j++) {
                        if (pkColumns[j].columnName == null) {
                            pkColumns[j].columnName = pkColumns[j].column.getName();
                        }
                        if (c.getName().equals(pkColumns[j].columnName)) {
                            c.setNullable(false);
                        }

                        c.setPrimaryKey(true);
                    }
                }
            }
            final ObjectArray sequences = new ObjectArray();
            for (int i = 0; i < columns.size(); i++) {
                final Column c = (Column) columns.get(i);

                if (fullTableName.startsWith("H2O.H2O") && i == 0) { // XXX
                    // nasty
                    // h2o-specific
                    // auto-increment
                    // hack.
                    c.setAutoIncrement(true, 1, 1);
                }
                if (c.getAutoIncrement()) {
                    final int objId = getObjectId(true, true);
                    c.convertAutoIncrementToSequence(session, getSchema(), objId, temporary);
                }
                final Sequence seq = c.getSequence();
                if (seq != null) {
                    sequences.add(seq);
                }
            }
            final int id = getObjectId(true, true);

            table = getSchema().createTable(tableName, id, columns, persistent, clustered, headPos);
            table.setComment(comment);
            table.setTemporary(temporary);
            table.setGlobalTemporary(globalTemporary);
            if (temporary && !globalTemporary) {
                if (onCommitDrop) {
                    table.setOnCommitDrop(true);
                }
                if (onCommitTruncate) {
                    table.setOnCommitTruncate(true);
                }
                session.addLocalTempTable(table);
            }
            else {
                db.addSchemaObject(session, table);
            }

            try {
                for (int i = 0; i < columns.size(); i++) {
                    final Column c = (Column) columns.get(i);
                    c.prepareExpression(session);
                }
                for (int i = 0; i < sequences.size(); i++) {
                    final Sequence sequence = (Sequence) sequences.get(i);
                    table.addSequence(sequence);
                }
                for (int i = 0; i < constraintCommands.size(); i++) {
                    final Prepared command = (Prepared) constraintCommands.get(i);
                    command.update();
                }
                if (asQuery != null) {
                    final boolean old = session.getUndoLogEnabled();
                    try {
                        session.setUndoLogEnabled(false);
                        Insert insert = null;
                        insert = new Insert(session, true);
                        insert.setQuery(asQuery);
                        insert.setTable(table);
                        insert.prepare();
                        insert.update();
                    }
                    finally {
                        session.setUndoLogEnabled(old);
                    }
                }

            }
            catch (final SQLException e) {
                db.checkPowerOff();
                db.removeSchemaObject(session, table);
                throw e;
            }

        }
        else {
            table = getSchema().findLocalTableOrView(session, tableName);
        }

        try {
            /*
             * Copy over the data that we have stored in the 'inserts' set. This section of code loops through that set and does some fairly
             * primitive string splitting to get each value.
             */

            if (!empty && inserts.size() > 1) { // the first entry contains type
                // info
                final Insert command = new Insert(session, true);

                command.setTable(table);

                final Column[] columnArray = new Column[columns.size()];
                columns.toArray(columnArray);
                command.setColumns(columnArray);

                final ArrayList<Integer> types = new ArrayList<Integer>();

                boolean firstRun = true;
                for (String statement : inserts) {

                    final ObjectArray values = new ObjectArray();
                    statement = statement.substring(0, statement.length() - 1);

                    int i = 0;
                    for (String part : statement.split(Constants.REPLICATION_DELIMITER)) {
                        part = part.trim();
                        if (firstRun) {
                            types.add(new Integer(part));
                        }
                        else {

                            if (part.startsWith("'") && part.endsWith("'")) {
                                part = part.substring(1, part.length() - 1);
                            }
                            final ValueString val = ValueString.get(part);

                            values.add(ValueExpression.get(val.convertTo(types.get(i++))));
                        }

                    }

                    if (firstRun) {
                        firstRun = false;
                    }
                    else {
                        final Expression[] expr = new Expression[values.size()];
                        values.toArray(expr);
                        command.addRow(expr);
                    }

                }

                command.update();
            }

            if (createEntirelyNewReplica && !empty) { //if this a CREATE EMPTY REPLICA statement the table manager hasn't been created yet.
                // #############################
                // Add to Table Manager.
                // #############################

                final TableInfo ti = new TableInfo(tableName, getSchema().getName(), table.getModificationId(), tableSet, table.getTableType(), db.getURL());

                if (!db.isTableLocal(getSchema())) {
                    TableManagerRemote tableManager = db.getSystemTableReference().lookup(getSchema().getName() + "." + tableName, true);

                    if (tableManager == null) {
                        throw new SQLException("Error creating replica for " + tableName + ". Table Manager not found.");
                    }
                    else {
                        try {
                            tableManager.addReplicaInformation(ti);
                        }
                        catch (final MovedException e) {
                            // If this is an old cached reference contact the
                            // system table directly.
                            tableManager = db.getSystemTableReference().lookup(getSchema().getName() + "." + tableName, false);
                            tableManager.addReplicaInformation(ti);
                        }
                    }

                    H2OEventBus.publish(new H2OEvent(session.getDatabase().getURL().getURL(), DatabaseStates.REPLICA_CREATION, getSchema().getName() + "." + tableName));

                }
                else {
                    //					H2OEventBus.publish(new H2OEvent(this.session.getDatabase().getURL().getDbLocation(),
                    //							DatabaseStates.META_TABLE_REPLICA_CREATION, getSchema().getName() + "." + tableName));
                }
            }
        }
        catch (final SQLException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            throw e;
        }
        catch (final MovedException e) {
            db.checkPowerOff();
            db.removeSchemaObject(session, table);
            throw new SQLException("Table Manager has moved.");
        }

        if (next != null) {
            next.update();
        }

        return 0;
    }

    /**
     * Set the tableSet number for this table.
     * 
     * @param tableSet2
     */
    private void setTableSet(final int tableSet) {

        this.tableSet = tableSet;
    }

    /**
     * Push a command to a remote machine where it will be properly executed.
     * 
     * @param createReplica
     *            true, if the command being pushed is a create replica command. This results in any subsequent tables involved in the
     *            command also being pushed.
     * @return The result of the update.
     * @throws SQLException
     * @throws RemoteException
     */
    private int pushCommand(final String remoteDBLocation, final String query, final boolean createReplica) throws SQLException, RemoteException {

        try {
            final Database db = session.getDatabase();

            conn = db.getLinkConnection("org.h2.Driver", remoteDBLocation, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);

            int result = -1;

            synchronized (conn) {
                try {
                    final Statement stat = conn.getConnection().createStatement();

                    stat.execute(query);
                    result = stat.getUpdateCount();

                }
                catch (final SQLException e) {
                    conn.close();
                    conn = null;
                    e.printStackTrace();
                    throw e;
                }
            }

            if (next != null && createReplica) {
                next.update();
            }

            return result;

        }
        catch (final Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    private void generateColumnsFromQuery() {

        final int columnCount = asQuery.getColumnCount();
        final ObjectArray expressions = asQuery.getExpressions();
        for (int i = 0; i < columnCount; i++) {
            final Expression expr = (Expression) expressions.get(i);
            final int type = expr.getType();
            final String name = expr.getAlias();
            long precision = expr.getPrecision();
            final int displaySize = expr.getDisplaySize();
            final DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0 || dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE)) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 || dt.defaultScale > scale)) {
                scale = dt.defaultScale;
            }
            final Column col = new Column(name, type, precision, scale, displaySize);
            addColumn(col);
        }
    }

    /**
     * Sets the primary key columns, but also check if a primary key with different columns is already defined.
     * 
     * @param columns
     *            the primary key columns
     * @return true if the same primary key columns where already set
     */
    private boolean setPrimaryKeyColumns(final IndexColumn[] columns) throws SQLException {

        if (pkColumns != null) {
            if (columns.length != pkColumns.length) { throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY); }
            for (int i = 0; i < columns.length; i++) {
                final String columnName = columns[i].columnName == null ? columns[i].column.getName() : columns[i].columnName;
                if (!columnName.equals(pkColumns[i].columnName)) { throw Message.getSQLException(ErrorCode.SECOND_PRIMARY_KEY); }
            }
            return true;
        }
        pkColumns = columns;
        return false;
    }

    public void setPersistent(final boolean persistent) {

        this.persistent = persistent;
    }

    public void setGlobalTemporary(final boolean globalTemporary) {

        this.globalTemporary = globalTemporary;
    }

    /**
     * This temporary table is dropped on commit.
     */
    public void setOnCommitDrop() {

        onCommitDrop = true;
    }

    /**
     * This temporary table is truncated on commit.
     */
    public void setOnCommitTruncate() {

        onCommitTruncate = true;
    }

    public void setComment(final String comment) {

        this.comment = comment;
    }

    public void setClustered(final boolean clustered) {

        this.clustered = clustered;
    }

    /**
     * Get the primary location of the given table and get meta-data from that location along with the contents of the table.
     * 
     * @throws JdbcSQLException
     */
    public void readSQL() throws JdbcSQLException {

        try {
            connect(whereDataWillBeTakenFrom);
        }
        catch (final SQLException e) {
            ErrorHandling.errorNoEvent("whereDataWillBeTakenFrom: " + whereDataWillBeTakenFrom);
            e.printStackTrace();
            throw Message.getSQLException(ErrorCode.CONNECTION_BROKEN, tableName);
        }
    }

    private void connect(final String tableLocation) throws SQLException {

        final Database db = session.getDatabase();
        if (!empty) {
            conn = db.getLinkConnection("org.h2.Driver", tableLocation, PersistentSystemTable.USERNAME, PersistentSystemTable.PASSWORD);
            synchronized (conn) {
                try {
                    readMetaData();
                    getTableData();

                }
                catch (final SQLException e) {
                    conn.close();
                    conn = null;
                    throw e;
                }
            }
        }

    }

    /**
     * Get the data required to fill up this replica (to match the contents of the primary).
     * 
     * Data is then stored in the 'inserts' field.
     */
    private void getTableData() {

        // check if the table is accessible
        Statement stat = null;
        try {
            stat = conn.getConnection().createStatement();

            // String fullTableName = getSchema().getName() + "." + tableName;

            final ResultSet rs = stat.executeQuery("SCRIPT TABLE " + getSchema().getName() + "." + tableName);

            final List<String> inserts = new LinkedList<String>();

            while (rs.next()) {
                inserts.add(rs.getString(1));
            }

            this.inserts = inserts;

            rs.close();
        }
        catch (final SQLException e) {
            ErrorHandling.exceptionError(e, "Failed to fill replica.");
        }
        finally {
            JdbcUtils.closeSilently(stat);
        }
    }

    private void readMetaData() throws SQLException {

        final String originalSchema = getSchema().getName();

        final DatabaseMetaData meta = conn.getConnection().getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
        ResultSet rs = meta.getTables(null, originalSchema, tableName, null);

        rs.close();
        rs = meta.getColumns(null, originalSchema, tableName, null);

        int i = 0;
        final ObjectArray columnList = new ObjectArray();
        final HashMap<String, Column> columnMap = new HashMap<String, Column>();
        String catalog = null, schema = null;

        final Set<String> currentColumns = new HashSet<String>();
        /*
         * Iterate over column meta-data.
         */
        while (rs.next()) {
            final String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            final String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                // columnMap.clear(); //XXX this doesn't work in H2O.
                // columnList.clear();
                break;
            }
            String columnName = rs.getString("COLUMN_NAME");

            if (currentColumns.contains(columnName)) {
                // keys - this happens
                // with multiple
                // replicas.
                continue;
            }

            currentColumns.add(columnName);

            columnName = convertColumnName(columnName);
            final int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            final int scale = rs.getInt("DECIMAL_DIGITS");
            final int nullable = rs.getInt("NULLABLE");
            final int displaySize = MathUtils.convertLongToInt(precision);
            final int type = DataType.convertSQLTypeToValueType(sqlType);
            final Column col = new Column(columnName, type, precision, scale, displaySize);

            col.setNullable(nullable == 1);

            /*
             * Add this new column to the 'columns' field.
             */
            addColumn(col);

            columnList.add(col);
            columnMap.put(columnName, col);
        }
        rs.close();

        String qualifiedTableName = "";

        if (tableName.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
            qualifiedTableName = schema + "." + tableName;
        }
        else {
            qualifiedTableName = tableName;
        }

        /*
         * Try to access the table, to ensure it actually exists and can be queried. If no columns were added above, get them from the
         * meta-data which results from this query.
         */
        Statement stat = null;
        try {
            stat = conn.getConnection().createStatement();
            rs = stat.executeQuery("SELECT * FROM " + qualifiedTableName + " T WHERE 1=0");
            if (columns.size() == 0) {
                // alternative solution
                final ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount(); i++) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    final int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    final int scale = rsMeta.getScale(i + 1);
                    final int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    final int type = DataType.convertSQLTypeToValueType(sqlType);
                    final Column col = new Column(n, type, precision, scale, displaySize);
                    addColumn(col);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        }
        catch (final SQLException e) {
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new String[]{tableName}, e);
        }
        finally {
            JdbcUtils.closeSilently(stat);
        }

        /*
         * Get information on the primary keys for this table.
         */
        try {
            rs = meta.getPrimaryKeys(null, originalSchema, tableName);
        }
        catch (final SQLException e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ObjectArray list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = new ObjectArray();

            /*
             * Loop through each of the primary keys.
             */
            do {
                final int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }

                /*
                 * Loop through adding a new 'null' entry in the object array for each column that may be included later.
                 */
                while (list.size() < idx) {
                    list.add(null);
                }
                String columnName = rs.getString("COLUMN_NAME");
                columnName = convertColumnName(columnName);
                final Column column = columnMap.get(columnName);
                list.set(idx - 1, column);
            }
            while (rs.next());

            /*
             * Add this set of primary key columns to an index.
             */
            addConstraint(list, IndexType.createPrimaryKey(false, false));
            rs.close();
        }
        try {
            rs = meta.getIndexInfo(null, originalSchema, tableName, false, true);
        }
        catch (final SQLException e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = new ObjectArray();
        IndexType indexType = null;
        if (rs != null) {
            /*
             * Loop through all the indexes on this table
             */
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
                final String newIndexName = rs.getString("INDEX_NAME");
                if (pkName.equals(newIndexName)) {
                    continue;
                }
                if (indexName != null && !indexName.equals(newIndexName)) {
                    addConstraint(list, indexType);
                    indexName = null;
                }
                if (indexName == null) {
                    indexName = newIndexName;
                    list.clear();
                }
                final boolean unique = !rs.getBoolean("NON_UNIQUE");
                indexType = unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false);
                String columnName = rs.getString("COLUMN_NAME");
                columnName = convertColumnName(columnName);
                final Column column = columnMap.get(columnName);
                list.add(column);
                columnList.add(columnName);
            }
            rs.close();
        }
        if (indexName != null) {
            addConstraint(list, indexType);
        }
    }

    private void addConstraint(final ObjectArray list, final IndexType indexType) throws SQLException {

        /*
         * If this is a primary key constraint, do primary key stuff.
         */
        if (indexType.getPrimaryKey()) {
            final Column[] cols = new Column[list.size()];
            list.toArray(cols);

            /*
             * Set all primary key columns to be not nullable.
             */
            for (final Column c : cols) {
                if (c != null) {
                    c.setNullable(false);
                }
                else {
                    ErrorHandling.errorNoEvent("This column was null.");
                }
            }

            IndexColumn[] indexColumn = new IndexColumn[list.size()];
            indexColumn = IndexColumn.wrap(cols);

            final AlterTableAddConstraint pk = new AlterTableAddConstraint(session, getSchema(), false, internalQuery);
            pk.setType(AlterTableAddConstraint.PRIMARY_KEY);
            pk.setTableName(tableName);
            pk.setIndexColumns(indexColumn);

            addConstraintCommand(pk);
        }
    }

    private String convertColumnName(String columnName) {

        if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private long convertPrecision(final int sqlType, long precision) {

        // workaround for an Oracle problem
        // the precision reported by Oracle is 7 for a date column
        switch (sqlType) {
            case Types.DATE:
                precision = Math.max(ValueDate.PRECISION, precision);
                break;
            case Types.TIMESTAMP:
                precision = Math.max(ValueTimestamp.PRECISION, precision);
                break;
            case Types.TIME:
                precision = Math.max(ValueTime.PRECISION, precision);
                break;
        }
        return precision;
    }

    /**
     * Sets the location at which the replica will be located. If this method is called that location is not the local machine, and so the
     * command will be sent remotely to be executed.
     * 
     * @param replicationLocation
     *            The location of the remote database.
     */
    public void setReplicationLocation(final String replicationLocation) {

        whereReplicaWillBeCreated = replicationLocation;

        if (whereReplicaWillBeCreated != null && whereReplicaWillBeCreated.startsWith("'") && whereReplicaWillBeCreated.endsWith("'")) {
            whereReplicaWillBeCreated = whereReplicaWillBeCreated.substring(1, whereReplicaWillBeCreated.length() - 1);
        }

        if (next != null) {
            next.setReplicationLocation(whereReplicaWillBeCreated);
        }
    }

    /**
     * Sets the location at which the primary copy is located. If this method is called that location is not the local machine - this
     * location is used to get the meta-data and data from the given table.
     * 
     * @param originalLocation
     * @throws JdbcSQLException
     * @throws RemoteException
     */
    public void setOriginalLocation(final String originalLocation, final boolean contactSM) throws SQLException, RemoteException {

        contactSystemTableOnCompletion(contactSM);

        whereDataWillBeTakenFrom = originalLocation;

        if (whereDataWillBeTakenFrom != null && whereDataWillBeTakenFrom.startsWith("'") && whereDataWillBeTakenFrom.endsWith("'")) {
            whereDataWillBeTakenFrom = whereDataWillBeTakenFrom.substring(1, whereDataWillBeTakenFrom.length() - 1);
        }

        if (whereDataWillBeTakenFrom == null) {

            final ISystemTableReference sm = session.getDatabase().getSystemTableReference();

            TableManagerRemote tableManager;

            tableManager = sm.lookup(new TableInfo(tableName, getSchema().getName()), true);

            if (tableManager == null) {
                throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new TableInfo(tableName, getSchema().getName()).toString());
            }
            else {
                try {
                    whereDataWillBeTakenFrom = tableManager.getLocation().getOriginalURL();
                }
                catch (final MovedException e) {
                    // If this is an old cached reference contact the system
                    // table directly.
                    tableManager = sm.lookup(new TableInfo(tableName, getSchema().getName()), false);
                    try {
                        whereDataWillBeTakenFrom = tableManager.getLocation().getOriginalURL();
                    }
                    catch (final MovedException e1) {
                        // This should not happen. Abort the query.
                        throw new SQLException(e1.getMessage());
                    }
                }
            }
        }

        if (next != null) {
            next.setOriginalLocation(whereDataWillBeTakenFrom, contactSM);
        }
    }

    /**
     * @param next
     */
    public void addNextCreateReplica(final CreateReplica create) {

        if (next == null) {
            next = create;
        }
        else {
            next.addNextCreateReplica(create);
        }
    }

    @Override
    public String toString() {

        return tableName;
    }

    /**
     * @param b
     */
    public void contactSystemTableOnCompletion(final boolean b) {

    }

}
