/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.text.Collator;

import org.h2.command.Prepared;
import org.h2.compress.Compressor;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.engine.Setting;
import org.h2.expression.Expression;
import org.h2.expression.ValueExpression;
import org.h2.message.Message;
import org.h2.result.LocalResult;
import org.h2.schema.Schema;
import org.h2.table.ReplicaSet;
import org.h2.tools.CompressTool;
import org.h2.util.StringUtils;
import org.h2.value.CompareMode;
import org.h2.value.ValueInt;

/**
 * This class represents the statement SET
 */
public class Set extends Prepared {

    private final int type;

    private Expression expression;

    private String stringValue;

    private String[] stringValueList;

    public Set(final Session session, final int type, final boolean internalQuery) {

        super(session, internalQuery);
        this.type = type;
    }

    public void setString(final String v) {

        stringValue = v;
    }

    @Override
    public boolean isTransactional() {

        switch (type) {
            case SetTypes.VARIABLE:
            case SetTypes.QUERY_TIMEOUT:
            case SetTypes.LOCK_TIMEOUT:
            case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
            case SetTypes.TRACE_LEVEL_FILE:
            case SetTypes.THROTTLE:
            case SetTypes.SCHEMA:
            case SetTypes.SCHEMA_SEARCH_PATH:
                return true;
            default:
        }
        return false;
    }

    @Override
    public int update() throws SQLException {

        // Value v = expr.getValue();
        final Database database = session.getDatabase();
        final String name = SetTypes.getTypeName(type);
        switch (type) {
            case SetTypes.ALLOW_LITERALS: {
                session.getUser().checkAdmin();
                final int value = getIntValue();
                if (value < 0 || value > 2) { throw Message.getInvalidValueException("" + getIntValue(), "ALLOW_LITERALS"); }
                database.setAllowLiterals(value);
                addOrUpdateSetting(name, null, value);
                break;
            }
            case SetTypes.CACHE_SIZE:
                session.getUser().checkAdmin();
                database.setCacheSize(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.CLUSTER: {
                session.getUser().checkAdmin();
                database.setCluster(StringUtils.quoteStringSQL(stringValue));
                addOrUpdateSetting(name, StringUtils.quoteStringSQL(stringValue), 0);
                break;
            }
            case SetTypes.COLLATION: {
                session.getUser().checkAdmin();
                final ReplicaSet replicaSet = database.getFirstUserTable();
                if (replicaSet != null) { throw Message.getSQLException(ErrorCode.COLLATION_CHANGE_WITH_DATA_TABLE_1, replicaSet.getSQL()); }
                CompareMode compareMode;
                final StringBuilder buff = new StringBuilder(stringValue);
                if (stringValue.equals(CompareMode.OFF)) {
                    compareMode = new CompareMode(null, null, 0);
                }
                else {
                    final Collator coll = CompareMode.getCollator(stringValue);
                    compareMode = new CompareMode(coll, stringValue, SysProperties.getCollatorCacheSize());
                    buff.append(" STRENGTH ");
                    if (getIntValue() == Collator.IDENTICAL) {
                        buff.append("IDENTICAL");
                    }
                    else if (getIntValue() == Collator.PRIMARY) {
                        buff.append("PRIMARY");
                    }
                    else if (getIntValue() == Collator.SECONDARY) {
                        buff.append("SECONDARY");
                    }
                    else if (getIntValue() == Collator.TERTIARY) {
                        buff.append("TERTIARY");
                    }
                    coll.setStrength(getIntValue());
                }
                addOrUpdateSetting(name, buff.toString(), 0);
                database.setCompareMode(compareMode);
                break;
            }
            case SetTypes.COMPRESS_LOB: {
                session.getUser().checkAdmin();
                final int algo = CompressTool.getInstance().getCompressAlgorithm(stringValue);
                database.setLobCompressionAlgorithm(algo == Compressor.NO ? null : stringValue);
                addOrUpdateSetting(name, stringValue, 0);
                break;
            }
            case SetTypes.CREATE_BUILD: {
                session.getUser().checkAdmin();
                if (database.isStarting()) {
                    // just ignore the command if not starting
                    // this avoids problems when running recovery scripts
                    final int value = getIntValue();
                    addOrUpdateSetting(name, null, value);
                }
                break;
            }
            case SetTypes.DATABASE_EVENT_LISTENER: {
                session.getUser().checkAdmin();
                database.setEventListenerClass(stringValue);
                break;
            }
            case SetTypes.DB_CLOSE_DELAY: {
                session.getUser().checkAdmin();
                database.setCloseDelay(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.DEFAULT_LOCK_TIMEOUT:
                session.getUser().checkAdmin();
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.DEFAULT_TABLE_TYPE:
                session.getUser().checkAdmin();
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.EXCLUSIVE: {
                session.getUser().checkAdmin();
                final int value = getIntValue();
                database.setExclusiveSession(value == 1 ? session : null);
                break;
            }
            case SetTypes.IGNORECASE:
                session.getUser().checkAdmin();
                database.setIgnoreCase(getIntValue() == 1);
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.LOCK_MODE:
                session.getUser().checkAdmin();
                database.setLockMode(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.LOCK_TIMEOUT:
                session.setLockTimeout(getIntValue());
                break;
            case SetTypes.LOG: {
                final int value = getIntValue();
                if (value < 0 || value > 2) { throw Message.getInvalidValueException("" + getIntValue(), "LOG"); }
                if (value == 0) {
                    session.getUser().checkAdmin();
                }
                database.setLog(value);
                break;
            }
            case SetTypes.MAX_LENGTH_INPLACE_LOB: {
                if (getIntValue() < 0) { throw Message.getInvalidValueException("" + getIntValue(), "MAX_LENGTH_INPLACE_LOB"); }
                session.getUser().checkAdmin();
                database.setMaxLengthInplaceLob(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.MAX_LOG_SIZE:
                session.getUser().checkAdmin();
                database.setMaxLogSize((long) getIntValue() * 1024 * 1024);
                addOrUpdateSetting(name, null, getIntValue());
                break;
            case SetTypes.MAX_MEMORY_ROWS: {
                session.getUser().checkAdmin();
                database.setMaxMemoryRows(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.MAX_MEMORY_UNDO: {
                if (getIntValue() < 0) { throw Message.getInvalidValueException("" + getIntValue(), "MAX_MEMORY_UNDO"); }
                session.getUser().checkAdmin();
                database.setMaxMemoryUndo(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.MAX_OPERATION_MEMORY: {
                session.getUser().checkAdmin();
                final int value = getIntValue();
                database.setMaxOperationMemory(value);
                break;
            }
            case SetTypes.MODE:
                session.getUser().checkAdmin();
                final Mode mode = Mode.getInstance(stringValue);
                if (mode == null) { throw Message.getSQLException(ErrorCode.UNKNOWN_MODE_1, stringValue); }
                database.setMode(mode);
                break;
            case SetTypes.MULTI_THREADED: {
                // Ignore, multi-threading is hard-wired on.
                break;
            }
            case SetTypes.MVCC: {
                // Ignore, multi-version is hard-wired off.
                break;
            }
            case SetTypes.OPTIMIZE_REUSE_RESULTS: {
                session.getUser().checkAdmin();
                database.setOptimizeReuseResults(getIntValue() != 0);
                break;
            }
            case SetTypes.QUERY_TIMEOUT: {
                final int value = getIntValue();
                session.setQueryTimeout(value);
                break;
            }
            case SetTypes.REFERENTIAL_INTEGRITY: {
                session.getUser().checkAdmin();
                final int value = getIntValue();
                if (value < 0 || value > 1) { throw Message.getInvalidValueException("" + getIntValue(), "REFERENTIAL_INTEGRITY"); }
                database.setReferentialIntegrity(value == 1);
                break;
            }
            case SetTypes.SCHEMA: {
                final Schema schema = database.getSchema(stringValue);
                session.setCurrentSchema(schema);
                break;
            }
            case SetTypes.SCHEMA_SEARCH_PATH: {
                session.setSchemaSearchPath(stringValueList);
                break;
            }
            case SetTypes.TRACE_LEVEL_FILE:
                session.getUser().checkAdmin();
                if (getCurrentObjectId() == 0) {
                    // don't set the property when opening the database
                    // this is for compatibility with older versions, because
                    // this setting was persistent
                    database.getTraceSystem().setLevelFile(getIntValue());
                }
                break;
            case SetTypes.TRACE_LEVEL_SYSTEM_OUT:
                session.getUser().checkAdmin();
                if (getCurrentObjectId() == 0) {
                    // don't set the property when opening the database
                    // this is for compatibility with older versions, because
                    // this setting was persistent
                    database.getTraceSystem().setLevelSystemOut(getIntValue());
                }
                break;
            case SetTypes.TRACE_MAX_FILE_SIZE: {
                session.getUser().checkAdmin();
                final int size = getIntValue() * 1024 * 1024;
                database.getTraceSystem().setMaxFileSize(size);
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            case SetTypes.THROTTLE: {
                if (getIntValue() < 0) { throw Message.getInvalidValueException("" + getIntValue(), "THROTTLE"); }
                session.setThrottle(getIntValue());
                break;
            }
            case SetTypes.UNDO_LOG: {
                final int value = getIntValue();
                if (value < 0 || value > 1) { throw Message.getInvalidValueException("" + getIntValue(), "UNDO_LOG"); }
                session.setUndoLogEnabled(value == 1);
                break;
            }
            case SetTypes.VARIABLE: {
                final Expression expr = expression.optimize(session);
                session.setVariable(stringValue, expr.getValue(session));
                break;
            }
            case SetTypes.WRITE_DELAY: {
                session.getUser().checkAdmin();
                database.setWriteDelay(getIntValue());
                addOrUpdateSetting(name, null, getIntValue());
                break;
            }
            default:
                Message.throwInternalError("type=" + type);
        }
        // the meta data information has changed
        database.getNextModificationDataId();
        return 0;
    }

    private int getIntValue() throws SQLException {

        expression = expression.optimize(session);
        return expression.getValue(session).getInt();
    }

    public void setInt(final int value) {

        expression = ValueExpression.get(ValueInt.get(value));
    }

    public void setExpression(final Expression expression) {

        this.expression = expression;
    }

    private void addOrUpdateSetting(final String name, final String s, final int v) throws SQLException {

        final Database database = session.getDatabase();
        if (database.getReadOnly()) { return; }
        Setting setting = database.findSetting(name);
        boolean addNew = false;
        if (setting == null) {
            addNew = true;
            final int id = getObjectId(false, true);
            setting = new Setting(database, id, name);
        }
        if (s != null) {
            if (!addNew && setting.getStringValue().equals(s)) { return; }
            setting.setStringValue(s);
        }
        else {
            if (!addNew && setting.getIntValue() == v) { return; }
            setting.setIntValue(v);
        }
        if (addNew) {
            database.addDatabaseObject(session, setting);
        }
        else {
            database.update(session, setting);
        }
    }

    @Override
    public boolean needRecompile() {

        return false;
    }

    @Override
    public LocalResult queryMeta() {

        return null;
    }

    public void setStringArray(final String[] list) {

        stringValueList = list;
    }

}
