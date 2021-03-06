package org.h2.table;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.SchemaObject;
import org.h2.util.ObjectArray;

import uk.ac.standrews.cs.nds.util.Diagnostic;
import uk.ac.standrews.cs.nds.util.DiagnosticLevel;

/**
 * H2O. Contains a set of replicas for a single table instance.
 * 
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class ReplicaSet {

    /**
     * The name of the table for which these replicas represent.
     */
    private final String tableName;

    /**
     * The set of all replicas of this table.
     */
    private Set<Table> replicas;

    /**
     * The primary copy of this table.
     */
    private Table primaryCopy;

    /**
     * Local copy of the data.
     */
    private Table localCopy;

    /**
     * Create a ReplicaSet for a new table in the system. The first instance of this table is automatically made the primary.
     * 
     * @param obj
     */
    public ReplicaSet(final SchemaObject obj) {

        final Table table = (Table) obj;

        replicas = new HashSet<Table>();

        replicas.add(table);

        primaryCopy = table;
        localCopy = table.isLocal() ? table : null;

        tableName = primaryCopy.getName();
    }

    /**
     * Get the Table instance associated with the primary copy of this table.
     * 
     * @return
     */
    public Table getPrimaryCopy() {

        return primaryCopy;
    }

    /**
     * Get a reference to a local copy of the data, if one exists. If not, null is returned.
     * 
     * @return
     */
    public Table getLocalCopy() {

        return localCopy;
    }

    public Set<Table> getAllCopies() {

        return replicas;
    }

    /**
     * Add a new replica for the table this replica set represents.
     * 
     * @param table
     *            The replica to be added.
     */
    public void addNewReplica(final SchemaObject obj) {

        final Table table = (Table) obj;

        localCopy = table.isLocal() ? table : null;

        replicas.add(table);

        if (primaryCopy == null) {
            primaryCopy = table;
        }
    }

    /**
     * The name of the table which these replicas represent.
     * 
     * @return Table name.
     */
    public String getTableName() {

        return tableName;
    }

    @Override
    public String toString() {

        return getTableName();
    }

    /**
     * Deconstruct this object.
     */
    public void removeAllCopies() {

        replicas = new HashSet<Table>();
        primaryCopy = null;
        localCopy = null;
    }

    /**
     * Return a single copy of the replica. Try to get a local copy first, but if none exists get another one.
     * 
     * @return
     */
    public Table getACopy() {

        //Diagnostic.traceNoEvent(DiagnosticLevel.FULL, tableName + ": Primary copy: " + primaryCopy + ". " + PrettyPrinter.toString(replicas));

        return getLocalCopy() == null ? getPrimaryCopy() : getLocalCopy();
    }

    /**
     * Remove a single copy of this data. If no more copies exist this method will return false; otherwise, true.
     * 
     * @param table
     *            Table to be removed.
     * @return True if other tables exist.
     */
    public boolean removeCopy(final Table table) {

        replicas.remove(table);

        if (table == localCopy) {
            localCopy = null;
        }
        if (table == primaryCopy) {
            primaryCopy = null;
        }

        return replicas.size() > 0;
    }

    /**
     * Remove a LinkedTable if one already exists and it doesn't point to the correct database URL
     * (the urls parameter). This is called from CreateLinkedTable if a linked table has 
     * to be created to another URL.
     * @param table
     * @param urls  If this is null it will remove the linked table regardless its location.
     * @return Returns true if the link table required (at the correct URL) already exists).
     */
    public boolean removeLinkedTable(final Table table, final Set<String> urls) {

        Table linkedTableToRemove = null;

        for (final Table tableReference : replicas) {
            if (tableReference instanceof TableLink) {
                final TableLink linkedTable = (TableLink) tableReference;

                if (urls == null || !urls.contains(linkedTable.getUrl())) {
                    linkedTableToRemove = tableReference;
                }

                break; //a linked table is like highlander: there can only ever be one.
            }
        }

        boolean anythingRemoved = replicas.remove(linkedTableToRemove);

        if (primaryCopy != null && primaryCopy instanceof TableLink) {
            final TableLink linkedTable = (TableLink) primaryCopy;

            if (urls == null || !urls.contains(linkedTable.getUrl())) {
                primaryCopy = null;

                anythingRemoved = true;

                Diagnostic.traceNoEvent(DiagnosticLevel.FULL, "Removing old linked table to machine at [" + linkedTable.getUrl() + "]");

            }
        }

        return anythingRemoved;
    }

    /**
     * Add dependancies to each copy.
     * 
     * @param set
     */
    public void addDependencies(final Set set) {

        if (replicas != null && replicas.size() > 0) {
            for (final Table table : replicas) {
                table.addDependencies(set);
            }
        }
    }

    /**
     * Get the SQL required to create the table and everything it relies on.
     * 
     * @return
     */
    public String getSQL() {

        if (getLocalCopy() != null) {
            return getLocalCopy().getSQL();
        }
        else if (getPrimaryCopy() != null) { return getPrimaryCopy().getSQL(); }
        return null;
    }

    /**
     * The SQL required to create the table
     * 
     * @return
     */
    public String getCreateSQL() {

        if (getLocalCopy() != null) {
            return getLocalCopy().getCreateSQL();
        }
        else if (getPrimaryCopy() != null) { return getPrimaryCopy().getCreateSQL(); }
        return null;
    }

    /**
     * @throws SQLException
     * 
     */
    public void checkRename() throws SQLException {

        if (replicas != null && replicas.size() > 0) {
            for (final Table table : replicas) {
                table.checkRename();
            }
        }
    }

    /**
     * @param newName
     * @throws SQLException
     */
    public void rename(final String newName) throws SQLException {

        if (replicas != null && replicas.size() > 0) {
            for (final Table table : replicas) {
                table.rename(newName);
            }
        }
    }

    public Index addIndex(final Session session, final String indexName, final int indexId, final IndexColumn[] cols, final IndexType indexType, final int headPos, final String comment) throws SQLException {

        Index index = null;
        for (final Table table : replicas) {
            final Index tempIndex = table.addIndex(session, indexName, indexId, cols, indexType, headPos, comment);

            if (table == localCopy) {
                index = tempIndex;
            }
        }
        return index;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#addRow(org.h2.engine.Session, org.h2.result.Row)
     */

    public void addRow(final Session session, final Row row) throws SQLException {

        for (final Table table : replicas) {
            table.addRow(session, row);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#canDrop()
     */

    public boolean canDrop() {

        if (localCopy == primaryCopy) {
            return primaryCopy.canDrop();
        }
        else {
            return false;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#canGetRowCount()
     */

    public boolean canGetRowCount() {

        if (localCopy != null) {
            return localCopy.canGetRowCount();
        }
        else if (primaryCopy != null) {
            return primaryCopy.canGetRowCount();
        }
        else {
            return ((Table) replicas.toArray()[0]).canGetRowCount();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#checkSupportAlter()
     */

    public void checkSupportAlter() throws SQLException {

        for (final Table table : replicas) {
            table.checkSupportAlter();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#close(org.h2.engine.Session)
     */

    public void close(final Session session) throws SQLException {

        for (final Table table : replicas) {
            table.close(session);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getIndexes()
     */

    public ObjectArray getIndexes() {

        Message.throwInternalError("Can't be called yet.");

        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getMaxDataModificationId()
     */

    public long getMaxDataModificationId() {

        long max = 0;

        for (final Table table : replicas) {
            if (max < table.getMaxDataModificationId()) {
                max = table.getMaxDataModificationId();
            }
        }
        return max;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getRowCount(org.h2.engine.Session)
     */

    public long getRowCount(final Session session) throws SQLException {

        if (localCopy != null) {
            return localCopy.getRowCount(session);
        }
        else if (primaryCopy != null) {
            return primaryCopy.getRowCount(session);
        }
        else {
            return ((Table) replicas.toArray()[0]).getRowCount(session);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getRowCountApproximation()
     */

    public long getRowCountApproximation() {

        if (localCopy != null) {
            return localCopy.getRowCountApproximation();
        }
        else if (primaryCopy != null) {
            return primaryCopy.getRowCountApproximation();
        }
        else {
            return ((Table) replicas.toArray()[0]).getRowCountApproximation();
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getScanIndex(org.h2.engine.Session)
     */

    public Index getScanIndex(final Session session) throws SQLException {

        Message.throwInternalError("Can't be called yet.");

        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getTableType()
     */

    public String getTableType() {

        Message.throwInternalError("Shouldn't be called.");
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#getUniqueIndex()
     */

    public Index getUniqueIndex() {

        Message.throwInternalError("Shouldn't be called.");
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#isLocal()
     */

    public boolean isLocal() {

        Message.throwInternalError("Shouldn't be called.");
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#isLockedExclusively()
     */

    public boolean isLockedExclusively() {

        Message.throwInternalError("Shouldn't be called.");
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#lock(org.h2.engine.Session, boolean, boolean)
     */

    public void lock(final Session session, final boolean exclusive, final boolean force) throws SQLException {

        Message.throwInternalError("Shouldn't be called.");
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#removeRow(org.h2.engine.Session, org.h2.result.Row)
     */

    public void removeRow(final Session session, final Row row) throws SQLException {

        for (final Table table : replicas) {
            table.removeRow(session, row);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#truncate(org.h2.engine.Session)
     */

    public void truncate(final Session session) throws SQLException {

        Message.throwInternalError("Can't be called yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.h2.table.Table#unlock(org.h2.engine.Session)
     */

    public void unlock(final Session s) {

        Message.throwInternalError("Can't be called yet.");
    }

    /*
     * (non-Javadoc)
     * @see org.h2.engine.DbObjectBase#getDropSQL()
     */

    public String getDropSQL() {

        if (localCopy != null) {
            return localCopy.getDropSQL();
        }
        else if (primaryCopy != null) {
            return primaryCopy.getDropSQL();
        }
        else {
            return ((Table) replicas.toArray()[0]).getDropSQL();
        }
    }

    /**
     * The number of replicas for this table.
     * 
     * @return Number of replicas.
     */
    public int size() {

        return replicas.size();
    }

}
