/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Storage;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * The scan index is not really an 'index' in the strict sense, because it can not be used for direct lookup. It can only be used to iterate
 * over all rows of a table. Each regular table has one such object, even if no primary key or indexes are defined.
 */
public class ScanIndex extends BaseIndex implements RowIndex {

    private int firstFree = -1;

    private ObjectArray rows = new ObjectArray();

    private Storage storage;

    private TableData tableData;

    private int rowCountDiff;

    private HashMap sessionRowCount;

    private HashSet delta;

    private long rowCount;

    public ScanIndex(final TableData table, final int id, final IndexColumn[] columns, final IndexType indexType) {

        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        tableData = table;
        if (!database.isPersistent() || id < 0) { return; }
        storage = database.getStorage(table, id, true);
        final int count = storage.getRecordCount();
        rowCount = count;
        table.setRowCount(count);
        trace.info("open existing " + table.getName() + " rows: " + count);
    }

    @Override
    public void remove(final Session session) throws SQLException {

        truncate(session);
        if (storage != null) {
            storage.truncate(session);
            database.removeStorage(storage.getId(), storage.getDiskFile());
        }
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        if (storage == null) {
            rows = new ObjectArray();
            firstFree = -1;
        }
        else {
            storage.truncate(session);
        }
        if (tableData.getContainsLargeObject() && tableData.getPersistent()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
    }

    @Override
    public String getCreateSQL() {

        return null;
    }

    @Override
    public void close(final Session session) {

        if (storage != null) {
            storage = null;
        }
    }

    @Override
    public Row getRow(final Session session, final int key) throws SQLException {

        if (storage != null) { return (Row) storage.getRecord(session, key); }
        return (Row) rows.get(key);
    }

    @Override
    public void add(final Session session, final Row row) throws SQLException {

        if (storage != null) {
            if (tableData.getContainsLargeObject()) {
                for (int i = 0; i < row.getColumnCount(); i++) {
                    final Value v = row.getValue(i);
                    final Value v2 = v.link(database, getId());
                    if (v2.isLinked()) {
                        session.unlinkAtCommitStop(v2);
                    }
                    if (v != v2) {
                        row.setValue(i, v2);
                    }
                }
            }
            storage.addRecord(session, row, Storage.ALLOCATE_POS);
        }
        else {
            // in-memory
            if (firstFree == -1) {
                final int key = rows.size();
                row.setPos(key);
                rows.add(row);
            }
            else {
                final int key = firstFree;
                final Row free = (Row) rows.get(key);
                firstFree = free.getPos();
                row.setPos(key);
                rows.set(key, row);
            }
            row.setDeleted(false);
        }
        rowCount++;
    }

    @Override
    public void remove(final Session session, final Row row) throws SQLException {

        if (storage != null) {
            storage.removeRecord(session, row.getPos());
            if (tableData.getContainsLargeObject()) {
                for (int i = 0; i < row.getColumnCount(); i++) {
                    final Value v = row.getValue(i);
                    if (v.isLinked()) {
                        session.unlinkAtCommit((ValueLob) v);
                    }
                }
            }
        }
        else {
            // in-memory
            if (!false && rowCount == 1) {
                rows = new ObjectArray();
                firstFree = -1;
            }
            else {
                final Row free = new Row(null, 0);
                free.setPos(firstFree);
                final int key = row.getPos();
                rows.set(key, free);
                firstFree = key;
            }
        }
        rowCount--;
    }

    @Override
    public Cursor find(final Session session, final SearchRow first, final SearchRow last) {

        return new ScanCursor(session, this, false);
    }

    @Override
    public double getCost(final Session session, final int[] masks) {

        long cost = tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET;
        if (storage != null) {
            cost *= 10;
        }
        return cost;
    }

    @Override
    public long getRowCount(final Session session) {

        return rowCount;
    }

    /**
     * Get the next row that is stored after this row.
     * 
     * @param session
     *            the session
     * @param row
     *            the current row or null to start the scan
     * @return the next row or null if there are no more rows
     */
    Row getNextRow(final Session session, Row row) throws SQLException {

        if (storage == null) {
            int key;
            if (row == null) {
                key = -1;
            }
            else {
                key = row.getPos();
            }
            while (true) {
                key++;
                if (key >= rows.size()) { return null; }
                row = (Row) rows.get(key);
                if (!row.isEmpty()) { return row; }
            }
        }
        final int pos = storage.getNext(row);
        if (pos < 0) { return null; }
        return (Row) storage.getRecord(session, pos);
    }

    @Override
    public int getColumnIndex(final Column col) {

        // the scan index cannot use any columns
        return -1;
    }

    @Override
    public void checkRename() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public boolean needRebuild() {

        return false;
    }

    @Override
    public boolean canGetFirstOrLast() {

        return false;
    }

    @Override
    public Cursor findFirstOrLast(final Session session, final boolean first) throws SQLException {

        throw Message.getUnsupportedException();
    }

    public Iterator getDelta() {

        return delta == null ? Collections.EMPTY_LIST.iterator() : delta.iterator();
    }

    @Override
    public long getRowCountApproximation() {

        return rowCount;
    }
}
