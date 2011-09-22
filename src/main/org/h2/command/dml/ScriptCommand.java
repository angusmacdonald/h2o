/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;

import org.h2.command.Parser;
import org.h2.constant.ErrorCode;
import org.h2.constant.LocationPreference;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.engine.Comment;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Right;
import org.h2.engine.Role;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.engine.User;
import org.h2.engine.UserAggregate;
import org.h2.engine.UserDataType;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.result.Row;
import org.h2.schema.Constant;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.table.Column;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.util.ByteUtils;
import org.h2.util.IOUtils;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueString;

/**
 * This class represents the statement SCRIPT
 */
public class ScriptCommand extends ScriptBase {

    private boolean passwords;

    private boolean data;

    private boolean settings;

    private boolean drop;

    private boolean simple;

    private LocalResult result;

    private byte[] lineSeparator;

    private byte[] buffer;

    private boolean tempLobTableCreated;

    private int nextLobId;

    private int lobBlockSize = Constants.IO_BUFFER_SIZE;

    /**
     * H2O. Whether the command is to print out the insert statements for a single table.
     */
    private boolean singleTable = false;

    private String tableName = null;

    private String schemaName = null;

    public ScriptCommand(final Session session, final boolean internalQuery) {

        super(session, internalQuery);
    }

    @Override
    public boolean isQuery() {

        return true;
    }

    // TODO lock all tables for 'script' command

    public void setData(final boolean data) {

        this.data = data;
    }

    public void setPasswords(final boolean passwords) {

        this.passwords = passwords;
    }

    public void setSettings(final boolean settings) {

        this.settings = settings;
    }

    public void setLobBlockSize(final long blockSize) {

        lobBlockSize = MathUtils.convertLongToInt(blockSize);
    }

    public void setDrop(final boolean drop) {

        this.drop = drop;
    }

    @Override
    public LocalResult queryMeta() throws SQLException {

        final LocalResult result = createResult();
        result.done();
        return result;
    }

    private LocalResult createResult() {

        final Expression[] expressions = new Expression[]{new ExpressionColumn(session.getDatabase(), new Column("SCRIPT", Value.STRING))};
        return new LocalResult(session, expressions, 1);
    }

    @Override
    public LocalResult query(final int maxrows) throws SQLException {

        session.getUser().checkAdmin();
        reset();
        try {
            result = createResult();
            deleteStore();
            openOutput();
            if (out != null) {
                buffer = new byte[Constants.IO_BUFFER_SIZE];
            }
            final Database db = session.getDatabase();

            if (singleTable) {

                /*
                 * Beginning of H2O code to get insert statements for a single table.
                 */
                if (schemaName == null) {
                    schemaName = session.getCurrentSchemaName();
                }
                final Table table = db.getSchema(schemaName).findTableOrView(session, tableName, LocationPreference.NO_PREFERENCE); // TODO
                // ensure
                // it
                // works
                // in
                // schema's
                // other
                // than
                // 'public'.

                if (table == null) { throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName); }

                table.lock(session, false, false);

                final String tableType = table.getTableType();

                if (Table.TABLE.equals(tableType)) {

                    final PlanItem plan = table.getBestPlanItem(session, null);
                    final Index index = plan.getIndex();
                    final Cursor cursor = index.find(session, null, null);
                    StringBuilder buff = new StringBuilder();

                    final Column[] columns = table.getColumns();

                    for (int j = 0; j < columns.length; j++) {
                        if (j > 0) {
                            buff.append(Constants.REPLICATION_DELIMITER);
                        }
                        buff.append(columns[j].getType());

                    }

                    add(buff.toString(), true);
                    buff = new StringBuilder();
                    final String ins = buff.toString();
                    buff = null;
                    while (cursor.next()) {
                        final Row row = cursor.get();
                        if (buff == null) {
                            buff = new StringBuilder(ins);
                        }

                        for (int j = 0; j < row.getColumnCount(); j++) {
                            if (j > 0) {
                                buff.append(Constants.REPLICATION_DELIMITER);
                            }
                            final Value v = row.getValue(j);
                            if (v.getPrecision() > lobBlockSize) {
                                int id;
                                if (v.getType() == Value.CLOB) {
                                    id = writeLobStream((ValueLob) v);
                                    buff.append("SYSTEM_COMBINE_CLOB(" + id + ")");
                                }
                                else if (v.getType() == Value.BLOB) {
                                    id = writeLobStream((ValueLob) v);
                                    buff.append("SYSTEM_COMBINE_BLOB(" + id + ")");
                                }
                                else {
                                    buff.append(v.getSQL());
                                }
                            }
                            else {
                                buff.append(v.getSQL());
                            }
                        }
                        buff.append("");

                        add(buff.toString(), true);
                        buff = null;

                    }

                }
                else {
                    throw new SQLException("H2O. Incompatible Table Type" + tableType);
                }

                // XXX to add indexes the mechanism used above would have to
                // become more complicated.
                // ObjectArray indexes = table.getIndexes();
                // for (int j = 0; indexes != null && j < indexes.size(); j++) {
                // Index index = (Index) indexes.get(j);
                // if (!index.getIndexType().getBelongsToConstraint()) {
                // add(index.getCreateSQL(), false);
                // }
                // }

                /*
                 * END of H2O code to get insert statements for a single table.
                 */
            }
            else {

                if (settings) {
                    final ObjectArray settings = db.getAllSettings();
                    for (int i = 0; i < settings.size(); i++) {
                        final Setting setting = (Setting) settings.get(i);
                        if (setting.getName().equals(SetTypes.getTypeName(SetTypes.CREATE_BUILD))) {
                            // don't add CREATE_BUILD to the script
                            // (it is only set when creating the database)
                            continue;
                        }
                        add(setting.getCreateSQL(), false);
                    }
                }
                if (out != null) {
                    add("", true);
                }
                final ObjectArray users = db.getAllUsers();
                for (int i = 0; i < users.size(); i++) {
                    final User user = (User) users.get(i);
                    add(user.getCreateSQL(passwords, true), false);
                }
                final ObjectArray roles = db.getAllRoles();
                for (int i = 0; i < roles.size(); i++) {
                    final Role role = (Role) roles.get(i);
                    add(role.getCreateSQL(true), false);
                }
                final ObjectArray schemas = db.getAllSchemas();
                for (int i = 0; i < schemas.size(); i++) {
                    final Schema schema = (Schema) schemas.get(i);
                    add(schema.getCreateSQL(), false);
                }
                final ObjectArray datatypes = db.getAllUserDataTypes();
                for (int i = 0; i < datatypes.size(); i++) {
                    final UserDataType datatype = (UserDataType) datatypes.get(i);
                    if (drop) {
                        add(datatype.getDropSQL(), false);
                    }
                    add(datatype.getCreateSQL(), false);
                }
                final ObjectArray constants = db.getAllSchemaObjects(DbObject.CONSTANT);
                for (int i = 0; i < constants.size(); i++) {
                    final Constant constant = (Constant) constants.get(i);
                    add(constant.getCreateSQL(), false);
                }
                final ObjectArray functionAliases = db.getAllFunctionAliases();
                for (int i = 0; i < functionAliases.size(); i++) {
                    final FunctionAlias alias = (FunctionAlias) functionAliases.get(i);
                    if (drop) {
                        add(alias.getDropSQL(), false);
                    }
                    add(alias.getCreateSQL(), false);
                }
                final ObjectArray aggregates = db.getAllAggregates();
                for (int i = 0; i < aggregates.size(); i++) {
                    final UserAggregate agg = (UserAggregate) aggregates.get(i);
                    if (drop) {
                        add(agg.getDropSQL(), false);
                    }
                    add(agg.getCreateSQL(), false);
                }

                final ObjectArray tables = new ObjectArray(db.getAllReplicas());

                // sort by id, so that views are after tables and views on views
                // after the base views
                tables.sort(new Comparator() {

                    @Override
                    public int compare(final Object o1, final Object o2) {

                        final Table t1 = (Table) o1;
                        final Table t2 = (Table) o2;
                        return t1.getId() - t2.getId();
                    }
                });
                for (int i = 0; i < tables.size(); i++) {
                    final Table table = (Table) tables.get(i);
                    table.lock(session, false, false);
                    final String sql = table.getCreateSQL();
                    if (sql == null) {
                        // null for metadata tables
                        continue;
                    }
                    if (drop) {
                        add(table.getDropSQL(), false);
                    }
                }
                final ObjectArray sequences = db.getAllSchemaObjects(DbObject.SEQUENCE);
                for (int i = 0; i < sequences.size(); i++) {
                    final Sequence sequence = (Sequence) sequences.get(i);
                    if (drop && !sequence.getBelongsToTable()) {
                        add(sequence.getDropSQL(), false);
                    }
                    add(sequence.getCreateSQL(), false);
                }
                for (int i = 0; i < tables.size(); i++) {
                    final Table table = (Table) tables.get(i);
                    table.lock(session, false, false);
                    final String sql = table.getCreateSQL();
                    if (sql == null) {
                        // null for metadata tables
                        continue;
                    }
                    final String tableType = table.getTableType();
                    add(sql, false);
                    if (Table.TABLE.equals(tableType)) {
                        if (table.canGetRowCount()) {
                            final String rowcount = "-- " + table.getRowCountApproximation() + " +/- SELECT COUNT(*) FROM " + table.getSQL();
                            add(rowcount, false);
                        }
                        if (data) {
                            final PlanItem plan = table.getBestPlanItem(session, null);
                            final Index index = plan.getIndex();
                            final Cursor cursor = index.find(session, null, null);
                            final Column[] columns = table.getColumns();
                            StringBuilder buff = new StringBuilder();
                            buff.append("INSERT INTO ");
                            buff.append(table.getSQL());
                            buff.append('(');
                            for (int j = 0; j < columns.length; j++) {
                                if (j > 0) {
                                    buff.append(", ");
                                }
                                buff.append(Parser.quoteIdentifier(columns[j].getName()));
                            }
                            buff.append(") VALUES");
                            if (!simple) {
                                buff.append('\n');
                            }
                            buff.append('(');
                            final String ins = buff.toString();
                            buff = null;
                            while (cursor.next()) {
                                final Row row = cursor.get();
                                if (buff == null) {
                                    buff = new StringBuilder(ins);
                                }
                                else {
                                    buff.append(",\n(");
                                }
                                for (int j = 0; j < row.getColumnCount(); j++) {
                                    if (j > 0) {
                                        buff.append(", ");
                                    }
                                    final Value v = row.getValue(j);
                                    if (v.getPrecision() > lobBlockSize) {
                                        int id;
                                        if (v.getType() == Value.CLOB) {
                                            id = writeLobStream((ValueLob) v);
                                            buff.append("SYSTEM_COMBINE_CLOB(" + id + ")");
                                        }
                                        else if (v.getType() == Value.BLOB) {
                                            id = writeLobStream((ValueLob) v);
                                            buff.append("SYSTEM_COMBINE_BLOB(" + id + ")");
                                        }
                                        else {
                                            buff.append(v.getSQL());
                                        }
                                    }
                                    else {
                                        buff.append(v.getSQL());
                                    }
                                }
                                buff.append(")");
                                if (simple || buff.length() > Constants.IO_BUFFER_SIZE) {
                                    add(buff.toString(), true);
                                    buff = null;
                                }
                            }
                            if (buff != null) {
                                add(buff.toString(), true);
                            }
                        }
                    }
                    final ObjectArray indexes = table.getIndexes();
                    for (int j = 0; indexes != null && j < indexes.size(); j++) {
                        final Index index = (Index) indexes.get(j);
                        if (!index.getIndexType().getBelongsToConstraint()) {
                            add(index.getCreateSQL(), false);
                        }
                    }
                }
                if (tempLobTableCreated) {
                    add("DROP TABLE IF EXISTS SYSTEM_LOB_STREAM", true);
                    add("CALL SYSTEM_COMBINE_BLOB(-1)", true);
                    add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_CLOB", true);
                    add("DROP ALIAS IF EXISTS SYSTEM_COMBINE_BLOB", true);
                    tempLobTableCreated = false;
                }
                final ObjectArray constraints = db.getAllSchemaObjects(DbObject.CONSTRAINT);
                constraints.sort(new Comparator() {

                    @Override
                    public int compare(final Object o1, final Object o2) {

                        final Constraint c1 = (Constraint) o1;
                        final Constraint c2 = (Constraint) o2;
                        return c1.compareTo(c2);
                    }
                });
                for (int i = 0; i < constraints.size(); i++) {
                    final Constraint constraint = (Constraint) constraints.get(i);
                    add(constraint.getCreateSQLWithoutIndexes(), false);
                }
                final ObjectArray triggers = db.getAllSchemaObjects(DbObject.TRIGGER);
                for (int i = 0; i < triggers.size(); i++) {
                    final TriggerObject trigger = (TriggerObject) triggers.get(i);
                    add(trigger.getCreateSQL(), false);
                }
                final ObjectArray rights = db.getAllRights();
                for (int i = 0; i < rights.size(); i++) {
                    final Right right = (Right) rights.get(i);
                    add(right.getCreateSQL(), false);
                }
                final ObjectArray comments = db.getAllComments();
                for (int i = 0; i < comments.size(); i++) {
                    final Comment comment = (Comment) comments.get(i);
                    add(comment.getCreateSQL(), false);
                }
            }
            if (out != null) {
                out.close();
            }

        }
        catch (final IOException e) {
            throw Message.convertIOException(e, getFileName());
        }
        finally {
            closeIO();
        }
        result.done();
        final LocalResult r = result;
        reset();
        return r;
    }

    private int writeLobStream(final ValueLob v) throws IOException, SQLException {

        if (!tempLobTableCreated) {
            add("CREATE TABLE IF NOT EXISTS SYSTEM_LOB_STREAM(ID INT, PART INT, CDATA VARCHAR, BDATA BINARY, PRIMARY KEY(ID, PART))", true);
            add("CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_CLOB FOR \"" + this.getClass().getName() + ".combineClob\"", true);
            add("CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_BLOB FOR \"" + this.getClass().getName() + ".combineBlob\"", true);
            tempLobTableCreated = true;
        }
        final int id = nextLobId++;
        switch (v.getType()) {
            case Value.BLOB: {
                final byte[] bytes = new byte[lobBlockSize];
                final InputStream in = v.getInputStream();
                try {
                    for (int i = 0;; i++) {
                        final StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                        buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", NULL, '");
                        final int len = IOUtils.readFully(in, bytes, 0, lobBlockSize);
                        if (len <= 0) {
                            break;
                        }
                        buff.append(ByteUtils.convertBytesToString(bytes, len));
                        buff.append("')");
                        final String sql = buff.toString();
                        add(sql, true);
                    }
                }
                finally {
                    IOUtils.closeSilently(in);
                }
                break;
            }
            case Value.CLOB: {
                final char[] chars = new char[lobBlockSize];
                final Reader in = v.getReader();
                try {
                    for (int i = 0;; i++) {
                        final StringBuilder buff = new StringBuilder(lobBlockSize * 2);
                        buff.append("INSERT INTO SYSTEM_LOB_STREAM VALUES(" + id + ", " + i + ", ");
                        final int len = IOUtils.readFully(in, chars, lobBlockSize);
                        if (len < 0) {
                            break;
                        }
                        buff.append(StringUtils.quoteStringSQL(new String(chars, 0, len)));
                        buff.append(", NULL)");
                        final String sql = buff.toString();
                        add(sql, true);
                    }
                }
                finally {
                    IOUtils.closeSilently(in);
                }
                break;
            }
            default:
                Message.throwInternalError("type:" + v.getType());
        }
        return id;
    }

    /**
     * Combine a BLOB. This method is called from the script. When calling with id -1, the file is deleted.
     * 
     * @param conn
     *            a connection
     * @param id
     *            the lob id
     * @return a stream for the combined data
     */
    public static InputStream combineBlob(final Connection conn, final int id) throws SQLException, IOException {

        if (id < 0) { return null; }
        final ResultSet rs = getLobStream(conn, "BDATA", id);
        return new InputStream() {

            private InputStream current;

            private boolean closed;

            @Override
            public int read() throws IOException {

                while (true) {
                    try {
                        if (current == null) {
                            if (closed) { return -1; }
                            if (!rs.next()) {
                                close();
                                return -1;
                            }
                            current = rs.getBinaryStream(1);
                            current = new BufferedInputStream(current);
                        }
                        final int x = current.read();
                        if (x >= 0) { return x; }
                        current = null;
                    }
                    catch (final SQLException e) {
                        Message.convertToIOException(e);
                    }
                }
            }

            @Override
            public void close() throws IOException {

                if (closed) { return; }
                closed = true;
                try {
                    rs.close();
                }
                catch (final SQLException e) {
                    Message.convertToIOException(e);
                }
            }
        };
    }

    /**
     * Combine a CLOB. This method is called from the script.
     * 
     * @param conn
     *            a connection
     * @param id
     *            the lob id
     * @return a reader for the combined data
     */
    public static Reader combineClob(final Connection conn, final int id) throws SQLException, IOException {

        if (id < 0) { return null; }
        final ResultSet rs = getLobStream(conn, "CDATA", id);
        return new Reader() {

            private Reader current;

            private boolean closed;

            @Override
            public int read() throws IOException {

                while (true) {
                    try {
                        if (current == null) {
                            if (closed) { return -1; }
                            if (!rs.next()) {
                                close();
                                return -1;
                            }
                            current = rs.getCharacterStream(1);
                            current = new BufferedReader(current);
                        }
                        final int x = current.read();
                        if (x >= 0) { return x; }
                        current = null;
                    }
                    catch (final SQLException e) {
                        Message.convertToIOException(e);
                    }
                }
            }

            @Override
            public void close() throws IOException {

                if (closed) { return; }
                closed = true;
                try {
                    rs.close();
                }
                catch (final SQLException e) {
                    Message.convertToIOException(e);
                }
            }

            @Override
            public int read(final char[] buffer, final int off, final int len) throws IOException {

                if (len == 0) { return 0; }
                int c = read();
                if (c == -1) { return -1; }
                buffer[off] = (char) c;
                int i = 1;
                for (; i < len; i++) {
                    c = read();
                    if (c == -1) {
                        break;
                    }
                    buffer[off + i] = (char) c;
                }
                return i;
            }
        };
    }

    private static ResultSet getLobStream(final Connection conn, final String column, final int id) throws SQLException {

        final PreparedStatement prep = conn.prepareStatement("SELECT " + column + " FROM SYSTEM_LOB_STREAM WHERE ID=? ORDER BY PART");
        prep.setInt(1, id);
        return prep.executeQuery();
    }

    private void reset() throws SQLException {

        result = null;
        buffer = null;
        lineSeparator = StringUtils.utf8Encode(SysProperties.LINE_SEPARATOR);
    }

    private void add(String s, final boolean insert) throws SQLException, IOException {

        if (s == null) { return; }
        s += ";";
        if (out != null) {
            final byte[] buff = StringUtils.utf8Encode(s);
            final int len = MathUtils.roundUp(buff.length + lineSeparator.length, Constants.FILE_BLOCK_SIZE);
            buffer = ByteUtils.copy(buff, buffer);

            if (len > buffer.length) {
                buffer = new byte[len];
            }
            System.arraycopy(buff, 0, buffer, 0, buff.length);
            for (int i = buff.length; i < len - lineSeparator.length; i++) {
                buffer[i] = ' ';
            }
            for (int j = 0, i = len - lineSeparator.length; i < len; i++, j++) {
                buffer[i] = lineSeparator[j];
            }
            out.write(buffer, 0, len);
            if (!insert) {
                final Value[] row = new Value[1];
                row[0] = ValueString.get(s);
                result.addRow(row);
            }
        }
        else {
            final Value[] row = new Value[1];
            row[0] = ValueString.get(s);
            result.addRow(row);
        }
    }

    public void setSimple(final boolean simple) {

        this.simple = simple;
    }

    /**
     * H2O. Called if the insert statements for a single table are required.
     * 
     * @param tableName
     *            The table for which the insert statements are required.
     */
    public void setTable(final String tableName) {

        singleTable = true;
        this.tableName = tableName;

    }

    /**
     * Set the schema name.
     * <p>
     * H2O. Called if the insert statements for a single table are required.
     * 
     * @param name
     */
    public void setSchema(final String name) {

        schemaName = name;
    }

}
