/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License, Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html). Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
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
 * This is the most common type of index, a b tree index. Only the data of the indexed columns are stored in the index.
 */
public class PageBtreeIndex extends BaseIndex {

    private PageStore store;

    private TableData tableData;

    private int headPos;

    private long rowCount;

    private boolean needRebuild;

    public PageBtreeIndex(final TableData table, final int id, final String indexName, final IndexColumn[] columns, final IndexType indexType, int headPos) throws SQLException {

        initBaseIndex(table, id, indexName, columns, indexType);
        tableData = table;
        if (!database.isPersistent() || id < 0) { return; }
        store = database.getPageStore();
        if (headPos == Index.EMPTY_HEAD) {
            // new index
            needRebuild = true;
            headPos = store.allocatePage();
            final PageBtreeLeaf root = new PageBtreeLeaf(this, headPos, Page.ROOT, store.createDataPage());
            store.updateRecord(root, true, root.data);
        }
        else {
            rowCount = getPage(headPos).getRowCount();
        }
        this.headPos = headPos;
        if (trace.isDebugEnabled()) {
            trace.debug("open " + rowCount);
        }
    }

    @Override
    public int getHeadPos() {

        return headPos;
    }

    @Override
    public void add(final Session session, final Row row) throws SQLException {

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
            PageBtree root = getPage(headPos);
            final int splitPoint = root.addRow(row);
            if (splitPoint == 0) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            final SearchRow pivot = root.getRow(splitPoint - 1);
            final PageBtree page1 = root;
            final PageBtree page2 = root.split(splitPoint);
            final int rootPageId = root.getPageId();
            final int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(headPos);
            page2.setParentPageId(headPos);
            final PageBtreeNode newRoot = new PageBtreeNode(this, rootPageId, Page.ROOT, store.createDataPage());
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
        rowCount++;
    }

    /**
     * Read the given page.
     * 
     * @param id
     *            the page id
     * @return the page
     */
    PageBtree getPage(final int id) throws SQLException {

        final Record rec = store.getRecord(id);
        if (rec != null) { return (PageBtree) rec; }
        final DataPage data = store.readPage(id);
        data.reset();
        final int parentPageId = data.readInt();
        final int type = data.readByte() & 255;
        PageBtree result;
        switch (type & ~Page.FLAG_LAST) {
            case Page.TYPE_BTREE_LEAF:
                result = new PageBtreeLeaf(this, id, parentPageId, data);
                break;
            case Page.TYPE_BTREE_NODE:
                result = new PageBtreeNode(this, id, parentPageId, data);
                break;
            case Page.TYPE_EMPTY:
                final PageBtreeLeaf empty = new PageBtreeLeaf(this, id, parentPageId, data);
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
    public Cursor findNext(final Session session, final SearchRow first, final SearchRow last) throws SQLException {

        return find(session, first, true, last);
    }

    @Override
    public Cursor find(final Session session, final SearchRow first, final SearchRow last) throws SQLException {

        return find(session, first, false, last);
    }

    private Cursor find(final Session session, final SearchRow first, final boolean bigger, final SearchRow last) throws SQLException {

        if (SysProperties.CHECK && store == null) { throw Message.getSQLException(ErrorCode.OBJECT_CLOSED); }
        final PageBtree root = getPage(headPos);
        final PageBtreeCursor cursor = new PageBtreeCursor(session, this, last);
        root.find(cursor, first, bigger);
        return cursor;
    }

    @Override
    public Cursor findFirstOrLast(final Session session, final boolean first) throws SQLException {

        throw Message.getUnsupportedException();
    }

    @Override
    public double getCost(final Session session, final int[] masks) {

        return 10 * getCostRangeIndex(masks, tableData.getRowCount(session));
    }

    @Override
    public boolean needRebuild() {

        return needRebuild;
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
            final PageBtree root = getPage(headPos);
            root.remove(row);
            rowCount--;
            final int todoReuseKeys;
        }
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
        final PageBtreeLeaf root = new PageBtreeLeaf(this, headPos, Page.ROOT, store.createDataPage());
        store.updateRecord(root, true, null);
        rowCount = 0;
    }

    @Override
    public void checkRename() {

        // ok
    }

    /**
     * Get a row from the data file.
     * 
     * @param session
     *            the session
     * @param key
     *            the row key
     * @return the row
     */
    public Row getRow(final Session session, final int key) throws SQLException {

        return tableData.getRow(session, key);
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
    public void close(final Session session) throws SQLException {

        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        final int todoWhyRequired;
        // store = null;
        final int writeRowCount;
    }

    /**
     * Read a row from the data page at the given offset.
     * 
     * @param data
     *            the data
     * @param offset
     *            the offset
     * @return the row
     */
    SearchRow readRow(final DataPage data, final int offset) throws SQLException {

        data.setPos(offset);
        final SearchRow row = table.getTemplateSimpleRow(columns.length == 1);
        row.setPos(data.readInt());
        for (final Column column : columns) {
            final int idx = column.getColumnId();
            row.setValue(idx, data.readValue());
        }
        return row;
    }

    /**
     * Write a row to the data page at the given offset.
     * 
     * @param data
     *            the data
     * @param offset
     *            the offset
     * @param row
     *            the row to write
     */
    void writeRow(final DataPage data, final int offset, final SearchRow row) throws SQLException {

        data.setPos(offset);
        data.writeInt(row.getPos());
        for (final Column column : columns) {
            final int idx = column.getColumnId();
            data.writeValue(row.getValue(idx));
        }
    }

    /**
     * Get the size of a row (only the part that is stored in the index).
     * 
     * @param dummy
     *            a dummy data page to calculate the size
     * @param row
     *            the row
     * @return the number of bytes
     */
    int getRowSize(final DataPage dummy, final SearchRow row) throws SQLException {

        int rowsize = DataPage.LENGTH_INT;
        for (final Column column : columns) {
            final Value v = row.getValue(column.getColumnId());
            rowsize += dummy.getValueLen(v);
        }
        return rowsize;
    }

}
