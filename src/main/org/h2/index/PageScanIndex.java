/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.DataPage;
import org.h2.store.PageStore;
import org.h2.store.Record;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.value.Value;
import org.h2.value.ValueLob;

/**
 * The scan index allows to access a row by key. It can be used to iterate over all rows of a table. Each regular table has one such object,
 * even if no primary key or indexes are defined.
 */
public class PageScanIndex extends BaseIndex implements RowIndex {

    private PageStore store;

    private TableData tableData;

    private int headPos;

    private int lastKey;

    private long rowCount;

    public PageScanIndex(final TableData table, final int id, final IndexColumn[] columns, final IndexType indexType, int headPos) throws SQLException {

        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);

        tableData = table;
        if (!database.isPersistent() || id < 0) { return; }
        store = database.getPageStore();
        if (headPos == Index.EMPTY_HEAD) {
            // new table
            headPos = store.allocatePage();
            final PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
            store.updateRecord(root, true, root.data);

        }
        else {
            final PageData root = getPage(headPos);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
            // could have been created before, but never committed
            store.updateRecord(root, false, null);
        }
        this.headPos = headPos;
        if (trace.isDebugEnabled()) {
            trace.debug("open " + rowCount);
        }
        table.setRowCount(rowCount);
    }

    @Override
    public int getHeadPos() {

        return headPos;
    }

    @Override
    public void add(final Session session, final Row row) throws SQLException {

        row.setPos(++lastKey);
        if (trace.isDebugEnabled()) {
            trace.debug("add " + row.getPos());
        }
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
        while (true) {
            PageData root = getPage(headPos);
            final int splitPoint = root.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            final int pivot = root.getKey(splitPoint - 1);
            final PageData page1 = root;
            final PageData page2 = root.split(splitPoint);
            final int rootPageId = root.getPageId();
            final int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            final PageDataNode newRoot = new PageDataNode(this, rootPageId, Page.ROOT, store.createDataPage());
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read the given page.
     * 
     * @param id
     *            the page id
     * @return the page
     */
    PageData getPage(final int id) throws SQLException {

        final Record rec = store.getRecord(id);
        if (rec != null) {
            if (rec instanceof PageDataLeafOverflow) {
                final int test;
                System.out.println("stop");
            }
            return (PageData) rec;
        }
        final DataPage data = store.readPage(id);
        data.reset();
        final int parentPageId = data.readInt();
        final int type = data.readByte() & 255;
        PageData result;
        switch (type & ~Page.FLAG_LAST) {
            case Page.TYPE_DATA_LEAF:
                result = new PageDataLeaf(this, id, parentPageId, data);
                break;
            case Page.TYPE_DATA_NODE:
                result = new PageDataNode(this, id, parentPageId, data);
                break;
            case Page.TYPE_EMPTY:
                final PageDataLeaf empty = new PageDataLeaf(this, id, parentPageId, data);
                return empty;
            default:
                throw Message.getSQLException(ErrorCode.FILE_CORRUPTED_1, "page=" + id + " type=" + type);
        }
        result.read();
        return result;
    }

    @Override
    public boolean canGetFirstOrLast() {

        return false;
    }

    @Override
    public Cursor find(final Session session, final SearchRow first, final SearchRow last) throws SQLException {

        final PageData root = getPage(headPos);
        return root.find();
    }

    @Override
    public Cursor findFirstOrLast(final Session session, final boolean first) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public double getCost(final Session session, final int[] masks) throws SQLException {

        final long cost = 10 * (tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    @Override
    public boolean needRebuild() {

        return false;
    }

    @Override
    public void remove(final Session session, final Row row) throws SQLException {

        if (trace.isDebugEnabled()) {
            trace.debug("remove " + row.getPos());
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                final Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit((ValueLob) v);
                }
            }
        }
        final int invalidateRowCount;
        // setChanged(session);
        if (rowCount == 1) {
            final int todoMaybeImprove;
            removeAllRows();
        }
        else {
            final int key = row.getPos();
            final PageData root = getPage(headPos);
            root.remove(key);
            rowCount--;
            final int todoReuseKeys;
            // if (key == lastKey - 1) {
            // lastKey--;
            // }
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    @Override
    public void remove(final Session session) throws SQLException {

        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        final int todo;
    }

    @Override
    public void truncate(final Session session) throws SQLException {

        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.getPersistent()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() throws SQLException {

        store.removeRecord(headPos);
        final int todoLogOldData;
        final int freePages;
        final PageDataLeaf root = new PageDataLeaf(this, headPos, Page.ROOT, store.createDataPage());
        store.updateRecord(root, true, null);
        rowCount = 0;
        lastKey = 0;
    }

    @Override
    public void checkRename() throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public Row getRow(final Session session, final int key) throws SQLException {

        final PageData root = getPage(headPos);
        return root.getRow(session, key);
    }

    PageStore getPageStore() {

        return store;
    }

    /**
     * Read a row from the data page at the given position.
     * 
     * @param data
     *            the data page
     * @return the row
     */
    Row readRow(final DataPage data) throws SQLException {

        return tableData.readRow(data);
    }

    @Override
    public long getRowCountApproximation() {

        return rowCount;
    }

    @Override
    public long getRowCount(final Session session) {

        return rowCount;
    }

    @Override
    public String getCreateSQL() {

        return null;
    }

    @Override
    public int getColumnIndex(final Column col) {

        // the scan index cannot use any columns
        // TODO it can if there is an INT primary key
        return -1;
    }

    @Override
    public void close(final Session session) throws SQLException {

        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        final int todoWhyNotClose;
        // store = null;
        final int writeRowCount;
    }

}
