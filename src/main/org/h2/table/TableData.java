/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import org.h2.api.DatabaseEventListener;
import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.constraint.Constraint;
import org.h2.constraint.ConstraintReferential;
import org.h2.engine.Constants;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.BtreeIndex;
import org.h2.index.Cursor;
import org.h2.index.HashIndex;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.PageBtreeIndex;
import org.h2.index.PageScanIndex;
import org.h2.index.RowIndex;
import org.h2.index.ScanIndex;
import org.h2.index.TreeIndex;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.store.DataPage;
import org.h2.store.Record;
import org.h2.store.RecordReader;
import org.h2.util.MathUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.Value;

import uk.ac.standrews.cs.nds.util.ErrorHandling;

/**
 * Most tables are an instance of this class. For this table, the data is stored in the database. The actual data is not kept here, instead
 * it is kept in the indexes. There is at least one index, the scan index.
 */
public class TableData extends Table implements RecordReader {

    private final boolean clustered;

    private RowIndex scanIndex;

    private long rowCount;

    private Session sessionHoldingExclusiveLock;

    private HashSet lockShared = new HashSet();

    private final Trace traceLock;

    private boolean globalTemporary;

    private final ObjectArray indexes = new ObjectArray();

    private long lastModificationId;

    private boolean containsLargeObject;

    public TableData(final Schema schema, final String tableName, final int id, final ObjectArray columns, final boolean persistent, final boolean clustered, final int headPos) throws SQLException {

        super(schema, id, tableName, persistent);
        final Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        setColumns(cols);
        this.clustered = clustered;
        if (!clustered) {
            if (SysProperties.PAGE_STORE && database.isPersistent()) {
                scanIndex = new PageScanIndex(this, id, IndexColumn.wrap(cols), IndexType.createScan(persistent), headPos);
            }
            else {
                scanIndex = new ScanIndex(this, id, IndexColumn.wrap(cols), IndexType.createScan(persistent));
            }
            indexes.add(scanIndex);
        }
        for (final Column col : cols) {
            if (DataType.isLargeObject(col.getType())) {
                containsLargeObject = true;
                memoryPerRow = Row.MEMORY_CALCULATE;
            }
        }
        traceLock = database.getTrace(Trace.LOCK);
    }

    @Override
    public int getHeadPos() {

        return scanIndex.getHeadPos();
    }

    @Override
    public void close(final Session session) throws SQLException {

        for (int i = 0; i < indexes.size(); i++) {
            final Index index = (Index) indexes.get(i);
            index.close(session);
        }
    }

    /**
     * Read the given row.
     * 
     * @param session
     *            the session
     * @param key
     *            the position of the row in the file
     * @return the row
     */
    public Row getRow(final Session session, final int key) throws SQLException {

        return scanIndex.getRow(session, key);
    }

    @Override
    public void addRow(final Session session, final Row row) throws SQLException {

        int i = 0;
        lastModificationId = database.getNextModificationDataId();
        try {
            for (; i < indexes.size(); i++) {
                final Index index = (Index) indexes.get(i);
                index.add(session, row);
                checkRowCount(session, index, 1);
            }
            rowCount++;
        }
        catch (final Throwable e) {
            try {
                while (--i >= 0) {
                    final Index index = (Index) indexes.get(i);
                    index.remove(session, row);
                    checkRowCount(session, index, 0);
                }
            }
            catch (final SQLException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error("Could not undo operation", e);
                throw e2;
            }
            throw Message.convert(e);
        }
    }

    private void checkRowCount(final Session session, final Index index, final int offset) {

        if (SysProperties.CHECK) {
            final long rc = index.getRowCount(session);
            if (rc != rowCount + offset) {
                ErrorHandling.error("rowCount expected " + (rowCount + offset) + " got " + rc + " " + getName() + "." + index.getName());
                Message.throwInternalError("rowCount expected " + (rowCount + offset) + " got " + rc + " " + getName() + "." + index.getName());
            }
        }
    }

    @Override
    public Index getScanIndex(final Session session) {

        return (Index) indexes.get(0);
    }

    @Override
    public Index getUniqueIndex() {

        for (int i = 0; i < indexes.size(); i++) {
            final Index idx = (Index) indexes.get(i);
            if (idx.getIndexType().getUnique()) { return idx; }
        }
        return null;
    }

    @Override
    public ObjectArray getIndexes() {

        return indexes;
    }

    @Override
    public Index addIndex(final Session session, final String indexName, final int indexId, final IndexColumn[] cols, final IndexType indexType, final int headPos, final String indexComment) throws SQLException {

        if (indexType.getPrimaryKey()) {
            for (final IndexColumn col : cols) {
                final Column column = col.column;

                if (column.getNullable()) { throw Message.getSQLException(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName()); }
                column.setPrimaryKey(true);
            }
        }
        Index index;
        if (getPersistent() && indexType.getPersistent()) {
            if (SysProperties.PAGE_STORE) {
                index = new PageBtreeIndex(this, indexId, indexName, cols, indexType, headPos);
            }
            else {
                index = new BtreeIndex(session, this, indexId, indexName, cols, indexType, headPos);
            }
        }
        else {
            if (indexType.getHash()) {
                index = new HashIndex(this, indexId, indexName, cols, indexType);
            }
            else {
                index = new TreeIndex(this, indexId, indexName, cols, indexType);
            }
        }

        if (index.needRebuild() && rowCount > 0) {
            try {
                final Index scan = getScanIndex(session);
                long remaining = scan.getRowCount(session);
                final long total = remaining;
                final Cursor cursor = scan.find(session, null, null);
                long i = 0;
                final int bufferSize = Constants.DEFAULT_MAX_MEMORY_ROWS;
                final ObjectArray buffer = new ObjectArray(bufferSize);
                while (cursor.next()) {
                    database.setProgress(DatabaseEventListener.STATE_CREATE_INDEX, getName() + ":" + index.getName(), MathUtils.convertLongToInt(i++), MathUtils.convertLongToInt(total));
                    final Row row = cursor.get();
                    // index.add(session, row);
                    buffer.add(row);
                    if (buffer.size() >= bufferSize) {
                        addRowsToIndex(session, buffer, index);
                    }
                    remaining--;
                }
                addRowsToIndex(session, buffer, index);
                if (SysProperties.CHECK && remaining != 0) {
                    Message.throwInternalError("rowcount remaining=" + remaining + " " + getName());
                }
            }
            catch (final SQLException e) {
                getSchema().freeUniqueName(indexName);
                try {
                    index.remove(session);
                }
                catch (final SQLException e2) {
                    // this could happen, for example on failure in the storage
                    // but if that is not the case it means
                    // there is something wrong with the database
                    trace.error("Could not remove index", e);
                    throw e2;
                }
                throw e;
            }
        }
        final boolean temporary = getTemporary();
        index.setTemporary(temporary);
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (temporary && !getGlobalTemporary()) {
                session.addLocalTempTableIndex(index);
            }
            else {
                database.addSchemaObject(session, index);
            }
            // Need to update, because maybe the index is rebuilt at startup,
            // and so the head pos may have changed, which needs to be stored
            // now.
            // addSchemaObject doesn't update the sys table at startup
            if (index.getIndexType().getPersistent() && !database.getReadOnly() && !database.getLog().containsInDoubtTransactions()) {
                // can not save anything in the log file if it contains in-doubt
                // transactions
                if (!SysProperties.PAGE_STORE) {
                    // must not do this when using the page store
                    // because recovery is not done yet
                    database.update(session, index);
                }
            }
        }
        indexes.add(index);
        setModified();
        return index;
    }

    @Override
    public boolean canGetRowCount() {

        return true;
    }

    private void addRowsToIndex(final Session session, final ObjectArray list, final Index index) throws SQLException {

        final Index idx = index;
        try {
            list.sort(new Comparator() {

                @Override
                public int compare(final Object o1, final Object o2) {

                    final Row r1 = (Row) o1;
                    final Row r2 = (Row) o2;
                    try {
                        return idx.compareRows(r1, r2);
                    }
                    catch (final SQLException e) {
                        throw Message.convertToInternal(e);
                    }
                }
            });
        }
        catch (final Exception e) {
            throw Message.convert(e);
        }
        for (int i = 0; i < list.size(); i++) {
            final Row row = (Row) list.get(i);
            index.add(session, row);
        }
        list.clear();
    }

    @Override
    public boolean canDrop() {

        return true;
    }

    @Override
    public long getRowCount(final Session session) {

        return rowCount;
    }

    @Override
    public void removeRow(final Session session, final Row row) throws SQLException {

        if (database == null) {
            database = session.getDatabase();
        }

        lastModificationId = database.getNextModificationDataId();
        int i = indexes.size() - 1;
        try {
            for (; i >= 0; i--) {
                final Index index = (Index) indexes.get(i);
                index.remove(session, row);
                checkRowCount(session, index, -1);
            }
            rowCount--;
        }
        catch (final Throwable e) {
            e.printStackTrace();
            try {
                while (++i < indexes.size()) {
                    final Index index = (Index) indexes.get(i);
                    index.add(session, row);
                    checkRowCount(session, index, 0);
                }
            }
            catch (final SQLException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error("Could not undo operation", e);
                throw e2;
            }
            throw Message.convert(e);
        }
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        lastModificationId = database.getNextModificationDataId();
        for (int i = indexes.size() - 1; i >= 0; i--) {
            final Index index = (Index) indexes.get(i);
            index.truncate(session);
            if (SysProperties.CHECK) {
                final long rc = index.getRowCount(session);
                if (rc != 0) {
                    Message.throwInternalError("rowCount expected 0 got " + rc);
                }
            }
        }
        rowCount = 0;
    }

    @Override
    boolean isLockedExclusivelyBy(final Session session) {

        return getSessionHoldingExclusiveLock() == session;
    }

    @Override
    public Session lock(final Session session, final boolean exclusive, final boolean force) throws SQLException {

        final int lockMode = database.getLockMode();

        if (lockMode != Constants.LOCK_MODE_OFF) {
            synchronized (database) {
                try {
                    return doLock(session, exclusive);
                }
                finally {
                    session.setWaitForLock(null);
                }
            }
        }

        return null;
    }

    private Session doLock(Session session, final boolean exclusive) throws SQLException {

        final long max = System.currentTimeMillis() + session.getLockTimeout();

        boolean checkDeadlock = false;
        while (true) {
            if (getSessionHoldingExclusiveLock() != null && session != getSessionHoldingExclusiveLock()) {
                /* 
                  * XXX H2O hack. It ensures that A-B-A communication doesn't lock up the DB (normally through the SYS table), as the returning update can use the same session
                  * as the originating update (which has all of the pertinent locks).
                  * 
                  * This happens when the PUBLIC.SYS table is altered (after almost every update through the Database.remoteMeta() [line 1266] method. This is called from 
                  * TableData.validateConvertUpdateSequence -> Column -> ... -> Sequence.flush(), after a user table has been updated. It seems that an updating user 
                  * transaction obtains an exclusive lock on the PUBLIC.SYS table.
                  */

                session = getSessionHoldingExclusiveLock();
            }

            if (getSessionHoldingExclusiveLock() == session) { return session; }

            if (exclusive) {
                if (getSessionHoldingExclusiveLock() == null) {
                    if (lockShared.isEmpty()) {
                        traceLock(session, exclusive, "added for");
                        session.addLock(this);
                        setSessionHoldingExclusiveLock(session);
                        return session;
                    }
                    else if (lockShared.size() == 1 && lockShared.contains(session)) {
                        traceLock(session, exclusive, "add (upgraded) for ");
                        setSessionHoldingExclusiveLock(session);
                        return session;
                    }
                }
            }
            else {
                if (getSessionHoldingExclusiveLock() == null) {

                    if (!lockShared.contains(session)) {
                        traceLock(session, exclusive, "ok");
                        session.addLock(this);
                        lockShared.add(session);
                    }
                    return session;
                }
            }
            session.setWaitForLock(this);
            if (checkDeadlock) {
                final ObjectArray sessions = checkDeadlock(session, null);
                if (sessions != null) { throw Message.getSQLException(ErrorCode.DEADLOCK_1, getDeadlockDetails(sessions)); }
            }
            else {
                // check for deadlocks from now on
                checkDeadlock = true;
            }
            final long now = System.currentTimeMillis();
            if (now >= max) {
                traceLock(session, exclusive, "timeout after " + session.getLockTimeout());

                System.err.println(getSessionHoldingExclusiveLock());
                throw Message.getSQLException(ErrorCode.LOCK_TIMEOUT_1, getName());
            }
            try {
                traceLock(session, exclusive, "waiting for");
                if (database.getLockMode() == Constants.LOCK_MODE_TABLE_GC) {
                    for (int i = 0; i < 20; i++) {
                        final long free = Runtime.getRuntime().freeMemory();
                        System.gc();
                        final long free2 = Runtime.getRuntime().freeMemory();
                        if (free == free2) {
                            break;
                        }
                    }
                }
                // don't wait too long so that deadlocks are detected early
                long sleep = Math.min(Constants.DEADLOCK_CHECK, max - now);
                if (sleep == 0) {
                    sleep = 1;
                }
                database.wait(sleep);
            }
            catch (final InterruptedException e) {
                // ignore
            }
        }
    }

    private String getDeadlockDetails(final ObjectArray sessions) {

        final StringBuilder buff = new StringBuilder();
        for (int i = 0; i < sessions.size(); i++) {
            buff.append('\n');
            final Session s = (Session) sessions.get(i);
            final Table lock = s.getWaitForLock();
            buff.append("Session ").append(s).append(" is waiting to lock ").append(lock);
            buff.append(" while locking ");
            final Table[] locks = s.getLocks();
            for (int j = 0; j < locks.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                final Table t = locks[j];
                buff.append(t);
                if (t instanceof TableData) {
                    if (((TableData) t).getSessionHoldingExclusiveLock() == s) {
                        buff.append(" (exclusive)");
                    }
                    else {
                        buff.append(" (shared)");
                    }
                }
            }
            buff.append('.');
        }
        return buff.toString();
    }

    @Override
    public ObjectArray checkDeadlock(final Session session, Session clash) {

        // only one deadlock check at any given time
        synchronized (TableData.class) {
            if (clash == null) {
                // verification is started
                clash = session;
            }
            else if (clash == session) {
                // we found a circle
                return new ObjectArray();
            }
            ObjectArray error = null;
            for (final Iterator it = lockShared.iterator(); it.hasNext();) {
                final Session s = (Session) it.next();
                if (s == session) {
                    // it doesn't matter if we have locked the object already
                    continue;
                }
                final Table t = s.getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(s, clash);
                    if (error != null) {
                        error.add(session);
                        break;
                    }
                }
            }
            if (error == null && getSessionHoldingExclusiveLock() != null) {
                final Table t = getSessionHoldingExclusiveLock().getWaitForLock();
                if (t != null) {
                    error = t.checkDeadlock(getSessionHoldingExclusiveLock(), clash);
                    if (error != null) {
                        error.add(session);
                    }
                }
            }
            return error;
        }
    }

    private void traceLock(final Session session, final boolean exclusive, final String s) {

        if (traceLock.isDebugEnabled()) {
            traceLock.debug(session.getId() + " " + (exclusive ? "exclusive write lock" : "shared read lock") + " " + s + " " + getName());
        }
    }

    @Override
    public String getDropSQL() {

        return "DROP TABLE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {

        final StringBuilder buff = new StringBuilder();
        buff.append("CREATE ");
        if (getTemporary()) {
            if (globalTemporary) {
                buff.append("GLOBAL ");
            }
            else {
                buff.append("LOCAL ");
            }
            buff.append("TEMPORARY ");
        }
        else if (getPersistent()) {
            buff.append("CACHED ");
        }
        else {
            buff.append("MEMORY ");
        }
        buff.append("TABLE ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(\n    ");
        for (int i = 0; i < columns.length; i++) {
            final Column column = columns[i];
            if (i > 0) {
                buff.append(",\n    ");
            }
            buff.append(column.getCreateSQL());
        }
        buff.append("\n)");
        return buff.toString();
    }

    @Override
    public boolean isLockedExclusively() {

        return getSessionHoldingExclusiveLock() != null;
    }

    @Override
    public void unlock(final Session s) {

        if (database != null) {
            traceLock(s, getSessionHoldingExclusiveLock() == s, "unlock");
            if (getSessionHoldingExclusiveLock() == s) {
                setSessionHoldingExclusiveLock(null);
            }
            if (lockShared != null && lockShared.size() > 0) {
                lockShared.remove(s);
            }
            // TODO lock: maybe we need we fifo-queue to make sure nobody
            // starves. check what other databases do
            synchronized (database) {
                if (database.getSessionCount() > 1) {
                    database.notifyAll();
                }
            }
        }
    }

    @Override
    public Record read(final Session session, final DataPage s) throws SQLException {

        return readRow(s);
    }

    /**
     * Read a row from the data page.
     * 
     * @param s
     *            the data page
     * @return the row
     */
    public Row readRow(final DataPage s) throws SQLException {

        final int len = s.readInt();
        final Value[] data = new Value[len];
        for (int i = 0; i < len; i++) {
            data[i] = s.readValue();
        }
        final Row row = new Row(data, memoryPerRow);
        return row;
    }

    /**
     * Set the row count of this table.
     * 
     * @param count
     *            the row count
     */
    public void setRowCount(final long count) {

        rowCount = count;
    }

    @Override
    public void removeChildrenAndResources(final Session session) throws SQLException {

        super.removeChildrenAndResources(session);
        // go backwards because database.removeIndex will call table.removeIndex
        while (indexes.size() > 1) {
            final Index index = (Index) indexes.get(1);
            if (index.getName() != null) {
                database.removeSchemaObject(session, index);
            }
        }
        if (SysProperties.CHECK) {
            final ObjectArray list = database.getAllSchemaObjects(DbObject.INDEX);
            for (int i = 0; i < list.size(); i++) {
                final Index index = (Index) list.get(i);
                if (index.getTable() == this) {
                    Message.throwInternalError("index not dropped: " + index.getName());
                }
            }
        }
        scanIndex.remove(session);
        database.removeMeta(session, getId());
        scanIndex = null;
        setSessionHoldingExclusiveLock(null);
        lockShared = null;
        invalidate();
    }

    @Override
    public String toString() {

        return getSQL();
    }

    @Override
    public void checkRename() {

        // ok
    }

    @Override
    public void checkSupportAlter() {

        // ok
    }

    @Override
    public boolean canTruncate() {

        final ObjectArray constraints = getConstraints();
        for (int i = 0; constraints != null && i < constraints.size(); i++) {
            final Constraint c = (Constraint) constraints.get(i);
            if (!c.getConstraintType().equals(Constraint.REFERENTIAL)) {
                continue;
            }
            final ConstraintReferential ref = (ConstraintReferential) c;
            if (ref.getRefTable() == this) { return false; }
        }
        return true;
    }

    @Override
    public String getTableType() {

        return Table.TABLE;
    }

    @Override
    public void setGlobalTemporary(final boolean globalTemporary) {

        this.globalTemporary = globalTemporary;
    }

    @Override
    public boolean getGlobalTemporary() {

        return globalTemporary;
    }

    @Override
    public long getMaxDataModificationId() {

        return lastModificationId;
    }

    @Override
    boolean getClustered() {

        return clustered;
    }

    public boolean getContainsLargeObject() {

        return containsLargeObject;
    }

    @Override
    public long getRowCountApproximation() {

        return scanIndex.getRowCountApproximation();
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#isLocal()
     */
    @Override
    public boolean isLocal() {

        return true;
    }

    public void setSessionHoldingExclusiveLock(final Session sessionHoldingExclusiveLock) {

        this.sessionHoldingExclusiveLock = sessionHoldingExclusiveLock;
    }

    public Session getSessionHoldingExclusiveLock() {

        return sessionHoldingExclusiveLock;
    }

}
